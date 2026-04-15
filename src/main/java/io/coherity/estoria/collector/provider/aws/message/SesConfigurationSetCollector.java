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
import software.amazon.awssdk.services.sesv2.model.ListConfigurationSetsRequest;
import software.amazon.awssdk.services.sesv2.model.ListConfigurationSetsResponse;
import software.amazon.awssdk.services.sesv2.model.SesV2Exception;

/**
 * Collects Amazon SES v2 configuration sets via the ListConfigurationSets API.
 */
@Slf4j
public class SesConfigurationSetCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "SesConfigurationSet";

	private SesV2Client sesV2Client;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of())
			.tags(Set.of("messaging", "ses", "email", "aws"))
			.build();

	public SesConfigurationSetCollector()
	{
		log.debug("SesConfigurationSetCollector created");
	}

	@Override
	public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
		log.debug("SesConfigurationSetCollector.collectEntities called");

		if (this.sesV2Client == null)
		{
			this.sesV2Client = AwsClientFactory.getInstance().getSesV2Client(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListConfigurationSetsRequest.Builder requestBuilder = ListConfigurationSetsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SesConfigurationSetCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListConfigurationSetsResponse response = this.sesV2Client.listConfigurationSets(requestBuilder.build());
			List<String> names     = response.configurationSets();
			String       nextToken = response.nextToken();

			log.debug("SesConfigurationSetCollector received {} configuration sets, nextToken={}",
				names != null ? names.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (names != null)
			{
				for (String name : names)
				{
					if (name == null || name.isBlank()) continue;

					String arn = ARNHelper.sesConfigurationSetArn(region, accountId, name);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("configurationSetName", name);
					attributes.put("accountId", accountId);
					attributes.put("region",    region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(name)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(name)
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
			log.error("SesConfigurationSetCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect SES configuration sets", e);
		}
		catch (Exception e)
		{
			log.error("SesConfigurationSetCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting SES configuration sets", e);
		}
	}
}
