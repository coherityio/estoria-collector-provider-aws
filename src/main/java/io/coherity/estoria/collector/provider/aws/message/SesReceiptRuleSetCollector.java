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
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.ses.model.ListReceiptRuleSetsRequest;
import software.amazon.awssdk.services.ses.model.ListReceiptRuleSetsResponse;
import software.amazon.awssdk.services.ses.model.ReceiptRuleSetMetadata;
import software.amazon.awssdk.services.ses.model.SesException;

/**
 * Collects Amazon SES (v1) receipt rule sets via the ListReceiptRuleSets API.
 *
 * Receipt rule sets are a SES v1 concept (not available in SES v2) and identify
 * the ordered set of receipt rules for incoming email processing.
 */
@Slf4j
public class SesReceiptRuleSetCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "SesReceiptRuleSet";


	public SesReceiptRuleSetCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("messaging", "ses", "email", "receipt", "aws")).build());
		log.debug("SesReceiptRuleSetCollector created");
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
		log.debug("SesReceiptRuleSetCollector.collectEntities called");

		SesClient sesClient = AwsClientFactory.getInstance().getSesClient(providerContext);

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			// ListReceiptRuleSets is paginated via nextToken on the request
			ListReceiptRuleSetsRequest.Builder requestBuilder = ListReceiptRuleSetsRequest.builder();

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SesReceiptRuleSetCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListReceiptRuleSetsResponse response = sesClient.listReceiptRuleSets(requestBuilder.build());
			List<ReceiptRuleSetMetadata> ruleSets  = response.ruleSets();
			String                       nextToken = response.nextToken();

			log.debug("SesReceiptRuleSetCollector received {} rule sets, nextToken={}",
				ruleSets != null ? ruleSets.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (ruleSets != null)
			{
				for (ReceiptRuleSetMetadata ruleSet : ruleSets)
				{
					if (ruleSet == null) continue;

					String name = ruleSet.name();
					if (name == null || name.isBlank()) continue;

					String arn = ARNHelper.sesReceiptRuleSetArn(region, accountId, name);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("ruleSetName", name);
					attributes.put("createdTimestamp",
						ruleSet.createdTimestamp() != null ? ruleSet.createdTimestamp().toString() : null);
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
						.rawPayload(ruleSet)
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
		catch (SesException e)
		{
			log.error("SesReceiptRuleSetCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect SES receipt rule sets", e);
		}
		catch (Exception e)
		{
			log.error("SesReceiptRuleSetCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting SES receipt rule sets", e);
		}
	}
}
