package io.coherity.estoria.collector.provider.aws.governance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksRequest;
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksResponse;
import software.amazon.awssdk.services.cloudformation.model.Stack;
import software.amazon.awssdk.services.cloudformation.model.Tag;

/**
 * Collects CloudFormation stacks via the DescribeStacks API.
 */
@Slf4j
public class CloudFormationStackCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "CloudFormationStack";

	private CloudFormationClient cloudFormationClient;

	public CloudFormationStackCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("governance", "cloudformation", "iac", "aws")).build());
		log.debug("CloudFormationStackCollector created");
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
		log.debug("CloudFormationStackCollector.collectEntities called");

		if (this.cloudFormationClient == null)
		{
			this.cloudFormationClient = AwsClientFactory.getInstance().getCloudFormationClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			DescribeStacksRequest.Builder requestBuilder = DescribeStacksRequest.builder();

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("CloudFormationStackCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeStacksResponse response = this.cloudFormationClient.describeStacks(requestBuilder.build());
			List<Stack> stacks    = response.stacks();
			String      nextToken = response.nextToken();

			log.debug("CloudFormationStackCollector received {} stacks, nextToken={}",
				stacks != null ? stacks.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (stacks != null)
			{
				for (Stack stack : stacks)
				{
					if (stack == null) continue;

					String stackId   = stack.stackId();
					String stackName = stack.stackName();
					String stackArn  = stackId != null ? stackId
						: "arn:aws:cloudformation:" + region + ":" + accountId + ":stack/" + stackName;

					Map<String, String> tagMap = stack.tags() == null ? Map.of()
						: stack.tags().stream().collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("stackId",            stackId);
					attributes.put("stackName",          stackName);
					attributes.put("stackArn",           stackArn);
					attributes.put("description",        stack.description());
					attributes.put("stackStatus",
						stack.stackStatus() != null ? stack.stackStatus().toString() : null);
					attributes.put("stackStatusReason",  stack.stackStatusReason());
					attributes.put("creationTime",
						stack.creationTime() != null ? stack.creationTime().toString() : null);
					attributes.put("lastUpdatedTime",
						stack.lastUpdatedTime() != null ? stack.lastUpdatedTime().toString() : null);
					attributes.put("deletionTime",
						stack.deletionTime() != null ? stack.deletionTime().toString() : null);
					attributes.put("roleArn",            stack.roleARN());
					attributes.put("parentId",           stack.parentId());
					attributes.put("rootId",             stack.rootId());
					attributes.put("driftStatus",
						stack.driftInformation() != null && stack.driftInformation().stackDriftStatus() != null
							? stack.driftInformation().stackDriftStatus().toString() : null);
					attributes.put("tags",               tagMap);
					attributes.put("accountId",          accountId);
					attributes.put("region",             region);

					String name = tagMap.getOrDefault("Name", stackName);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(stackArn)
							.qualifiedResourceName(stackArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(stack)
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
			log.error("CloudFormationStackCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect CloudFormation stacks", e);
		}
		catch (Exception e)
		{
			log.error("CloudFormationStackCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting CloudFormation stacks", e);
		}
	}
}
