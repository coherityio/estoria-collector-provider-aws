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
import software.amazon.awssdk.services.resourcegroups.ResourceGroupsClient;
import software.amazon.awssdk.services.resourcegroups.model.Group;
import software.amazon.awssdk.services.resourcegroups.model.GroupIdentifier;
import software.amazon.awssdk.services.resourcegroups.model.ListGroupsRequest;
import software.amazon.awssdk.services.resourcegroups.model.ListGroupsResponse;
import software.amazon.awssdk.services.resourcegroups.model.ResourceGroupsException;

/**
 * Collects AWS Resource Groups via the ListGroups API.
 */
@Slf4j
public class ResourceGroupsGroupCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "ResourceGroupsGroup";


	public ResourceGroupsGroupCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("governance", "resource-groups", "aws")).build());
		log.debug("ResourceGroupsGroupCollector created");
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
		log.debug("ResourceGroupsGroupCollector.collectEntities called");

		ResourceGroupsClient resourceGroupsClient = AwsClientFactory.getInstance().getResourceGroupsClient(providerContext);

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListGroupsRequest.Builder requestBuilder = ListGroupsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.maxResults(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("ResourceGroupsGroupCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListGroupsResponse response = resourceGroupsClient.listGroups(requestBuilder.build());
			List<GroupIdentifier> groupIds  = response.groupIdentifiers();
			String                nextToken = response.nextToken();

			log.debug("ResourceGroupsGroupCollector received {} groups, nextToken={}",
				groupIds != null ? groupIds.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (groupIds != null)
			{
				for (GroupIdentifier gid : groupIds)
				{
					if (gid == null) continue;

					String groupArn  = gid.groupArn();
					String groupName = gid.groupName();

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("groupArn",    groupArn);
					attributes.put("groupName",   groupName);
					attributes.put("accountId",   accountId);
					attributes.put("region",      region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(groupArn)
							.qualifiedResourceName(groupArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(groupName != null ? groupName : groupArn)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(gid)
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
		catch (ResourceGroupsException e)
		{
			log.error("ResourceGroupsGroupCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Resource Groups", e);
		}
		catch (Exception e)
		{
			log.error("ResourceGroupsGroupCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Resource Groups", e);
		}
	}
}
