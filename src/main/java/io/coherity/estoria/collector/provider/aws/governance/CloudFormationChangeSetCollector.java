package io.coherity.estoria.collector.provider.aws.governance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.cloudformation.model.ChangeSetSummary;
import software.amazon.awssdk.services.cloudformation.model.CloudFormationException;
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksResponse;
import software.amazon.awssdk.services.cloudformation.model.ListChangeSetsRequest;
import software.amazon.awssdk.services.cloudformation.model.ListChangeSetsResponse;
import software.amazon.awssdk.services.cloudformation.model.Stack;

/**
 * Collects CloudFormation change set summaries by enumerating all stacks
 * then calling ListChangeSets per stack.
 */
@Slf4j
public class CloudFormationChangeSetCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "CloudFormationChangeSet";


	public CloudFormationChangeSetCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(CloudFormationStackCollector.ENTITY_TYPE), Set.of("governance", "cloudformation", "iac", "aws")).build());
		log.debug("CloudFormationChangeSetCollector created");
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
		log.debug("CloudFormationChangeSetCollector.collectEntities called");

		CloudFormationClient cloudFormationClient = AwsClientFactory.getInstance().getCloudFormationClient(providerContext);

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			// Step 1: enumerate all stacks
			List<String> stackNames = new ArrayList<>();
			String stackNextToken = null;
			do
			{
				DescribeStacksRequest.Builder stackReq = DescribeStacksRequest.builder();
				if (stackNextToken != null) stackReq.nextToken(stackNextToken);
				DescribeStacksResponse stackResp = cloudFormationClient.describeStacks(stackReq.build());
				if (stackResp.stacks() != null)
				{
					for (Stack s : stackResp.stacks())
					{
						if (s != null && s.stackName() != null) stackNames.add(s.stackName());
					}
				}
				stackNextToken = stackResp.nextToken();
			}
			while (stackNextToken != null && !stackNextToken.isBlank());

			log.debug("CloudFormationChangeSetCollector found {} stacks to inspect", stackNames.size());

			// Step 2: list change sets per stack
			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			for (String stackName : stackNames)
			{
				String csNextToken = null;
				do
				{
					ListChangeSetsRequest.Builder csReq = ListChangeSetsRequest.builder().stackName(stackName);
					if (csNextToken != null) csReq.nextToken(csNextToken);

					ListChangeSetsResponse csResp = cloudFormationClient.listChangeSets(csReq.build());
					csNextToken = csResp.nextToken();

					List<ChangeSetSummary> changeSets = csResp.summaries();
					if (changeSets != null)
					{
						for (ChangeSetSummary cs : changeSets)
						{
							if (cs == null) continue;

							String changeSetId   = cs.changeSetId();
							String changeSetName = cs.changeSetName();
							String changeSetArn  = changeSetId != null ? changeSetId
								: "arn:aws:cloudformation:" + region + ":" + accountId
									+ ":changeSet/" + changeSetName + "/" + stackName;

							Map<String, Object> attributes = new HashMap<>();
							attributes.put("changeSetId",      changeSetId);
							attributes.put("changeSetName",    changeSetName);
							attributes.put("changeSetArn",     changeSetArn);
							attributes.put("stackId",          cs.stackId());
							attributes.put("stackName",        cs.stackName());
							attributes.put("description",      cs.description());
							attributes.put("status",
								cs.status() != null ? cs.status().toString() : null);
							attributes.put("statusReason",     cs.statusReason());
							attributes.put("executionStatus",
								cs.executionStatus() != null ? cs.executionStatus().toString() : null);
							attributes.put("creationTime",
								cs.creationTime() != null ? cs.creationTime().toString() : null);
							attributes.put("includeNestedStacks", cs.includeNestedStacks());
							attributes.put("accountId",        accountId);
							attributes.put("region",           region);

							CloudEntity entity = CloudEntity.builder()
								.entityIdentifier(EntityIdentifier.builder()
									.id(changeSetArn)
									.qualifiedResourceName(changeSetArn)
									.build())
								.entityType(ENTITY_TYPE)
								.name(changeSetName != null ? changeSetName : changeSetId)
								.collectorContext(collectorContext)
								.attributes(attributes)
								.rawPayload(cs)
								.collectedAt(now)
								.build();

							entities.add(entity);
						}
					}
				}
				while (csNextToken != null && !csNextToken.isBlank());
			}

			log.debug("CloudFormationChangeSetCollector collected {} change sets across {} stacks",
				entities.size(), stackNames.size());

			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());
			metadataValues.put("stackCount", stackNames.size());

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (CloudFormationException e)
		{
			log.error("CloudFormationChangeSetCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect CloudFormation change sets", e);
		}
		catch (Exception e)
		{
			log.error("CloudFormationChangeSetCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting CloudFormation change sets", e);
		}
	}
}
