package io.coherity.estoria.collector.provider.aws.governance;

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
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.cloudformation.model.ListStackSetsRequest;
import software.amazon.awssdk.services.cloudformation.model.ListStackSetsResponse;
import software.amazon.awssdk.services.cloudformation.model.StackSetStatus;
import software.amazon.awssdk.services.cloudformation.model.StackSetSummary;

/**
 * Collects CloudFormation StackSets via the ListStackSets API.
 */
@Slf4j
public class CloudFormationStackSetCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "CloudFormationStackSet";

	private CloudFormationClient cloudFormationClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of())
			.tags(Set.of("governance", "cloudformation", "iac", "aws"))
			.build();

	public CloudFormationStackSetCollector()
	{
		log.debug("CloudFormationStackSetCollector created");
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
		log.debug("CloudFormationStackSetCollector.collectEntities called");

		if (this.cloudFormationClient == null)
		{
			this.cloudFormationClient = AwsClientFactory.getInstance().getCloudFormationClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListStackSetsRequest.Builder requestBuilder = ListStackSetsRequest.builder()
				.status(StackSetStatus.ACTIVE);

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.maxResults(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("CloudFormationStackSetCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListStackSetsResponse response = this.cloudFormationClient.listStackSets(requestBuilder.build());
			List<StackSetSummary> stackSets = response.summaries();
			String               nextToken  = response.nextToken();

			log.debug("CloudFormationStackSetCollector received {} stack sets, nextToken={}",
				stackSets != null ? stackSets.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (stackSets != null)
			{
				for (StackSetSummary ss : stackSets)
				{
					if (ss == null) continue;

					String stackSetId   = ss.stackSetId();
					String stackSetName = ss.stackSetName();

					String stackSetArn = ARNHelper.cloudFormationStackSetArn(
					    region,
					    accountId,
					    stackSetName,
					    stackSetId);					
					
					Map<String, Object> attributes = new HashMap<>();
					attributes.put("stackSetId",          stackSetId);
					attributes.put("stackSetName",        stackSetName);
					attributes.put("stackSetArn",         stackSetArn);
					attributes.put("description",         ss.description());
					attributes.put("status",
						ss.status() != null ? ss.status().toString() : null);
					attributes.put("driftStatus",
						ss.driftStatus() != null ? ss.driftStatus().toString() : null);
					attributes.put("lastDriftCheckTimestamp",
						ss.lastDriftCheckTimestamp() != null ? ss.lastDriftCheckTimestamp().toString() : null);
					attributes.put("permissionModel",
						ss.permissionModel() != null ? ss.permissionModel().toString() : null);
					attributes.put("autoDeployment",
						ss.autoDeployment() != null
							? Map.of("enabled", ss.autoDeployment().enabled(),
								"retainStacksOnAccountRemoval", ss.autoDeployment().retainStacksOnAccountRemoval())
							: null);
					attributes.put("managedExecution",
						ss.managedExecution() != null ? ss.managedExecution().active() : null);
					attributes.put("accountId", accountId);
					attributes.put("region",    region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(stackSetArn)
							.qualifiedResourceName(stackSetArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(stackSetName)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(ss)
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
		catch (CloudFormationException e)
		{
			log.error("CloudFormationStackSetCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect CloudFormation stack sets", e);
		}
		catch (Exception e)
		{
			log.error("CloudFormationStackSetCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting CloudFormation stack sets", e);
		}
	}
}
