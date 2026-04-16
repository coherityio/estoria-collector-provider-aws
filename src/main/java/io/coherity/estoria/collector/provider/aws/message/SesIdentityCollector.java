package io.coherity.estoria.collector.provider.aws.message;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
import io.coherity.estoria.collector.provider.aws.AbstractAwsContextAwareCollector;
import io.coherity.estoria.collector.provider.aws.AccountScope;
import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.provider.aws.AwsSessionContext;
import io.coherity.estoria.collector.provider.aws.ContainmentScope;
import io.coherity.estoria.collector.provider.aws.EntityCategory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sesv2.SesV2Client;
import software.amazon.awssdk.services.sesv2.model.IdentityInfo;
import software.amazon.awssdk.services.sesv2.model.ListEmailIdentitiesRequest;
import software.amazon.awssdk.services.sesv2.model.ListEmailIdentitiesResponse;
import software.amazon.awssdk.services.sesv2.model.SesV2Exception;

/**
 * Collects Amazon SES v2 verified email and domain identities via the ListEmailIdentities API.
 */
@Slf4j
public class SesIdentityCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "SesIdentity";


	public SesIdentityCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("messaging", "ses", "email", "identity", "aws")).build());
		log.debug("SesIdentityCollector created");
	}

	@Override
	public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

	@Override
	public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }

	@Override
	public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

	@Override
	public CollectorCursor collectEntities(
		ProviderContext providerContext,
		AwsSessionContext awsSessionContext,
		CollectorContext collectorContext,
		CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("SesIdentityCollector.collectEntities called");

		SesV2Client sesV2Client = AwsClientFactory.getInstance().getSesV2Client(providerContext);

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListEmailIdentitiesRequest.Builder requestBuilder = ListEmailIdentitiesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SesIdentityCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListEmailIdentitiesResponse response = sesV2Client.listEmailIdentities(requestBuilder.build());
			List<IdentityInfo> identities = response.emailIdentities();
			String             nextToken  = response.nextToken();

			log.debug("SesIdentityCollector received {} identities, nextToken={}",
				identities != null ? identities.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (identities != null)
			{
				for (IdentityInfo identity : identities)
				{
					if (identity == null) continue;

					String identityName = identity.identityName();
					if (identityName == null || identityName.isBlank()) continue;

					String identityType = identity.identityType() != null ? identity.identityTypeAsString() : null;
					String arn          = ARNHelper.sesIdentityArn(region, accountId, identityName);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("identityName",          identityName);
					attributes.put("identityType",          identityType);
					attributes.put("sendingEnabled",        identity.sendingEnabled());
					attributes.put("verificationStatus",    identity.verificationStatusAsString());
					attributes.put("accountId",             accountId);
					attributes.put("region",                region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(identityName)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(identityName)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(identity)
						.collectedAt(now)
						.build();

					entities.add(entity);
				}
			}

			String finalNextToken = nextToken;
			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken()
				{
					return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
				}
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (SesV2Exception e)
		{
			log.error("SesIdentityCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect SES identities", e);
		}
		catch (Exception e)
		{
			log.error("SesIdentityCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting SES identities", e);
		}
	}
}
