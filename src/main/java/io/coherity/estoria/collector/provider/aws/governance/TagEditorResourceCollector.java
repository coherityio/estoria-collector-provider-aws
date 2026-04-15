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
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesRequest;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesResponse;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.ResourceGroupsTaggingApiException;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.ResourceTagMapping;

/**
 * Collects all tagged AWS resources in a region via the Resource Groups Tagging API (GetResources).
 * This provides a unified cross-service view of resources and their tags.
 */
@Slf4j
public class TagEditorResourceCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "TagEditorResource";

	private ResourceGroupsTaggingApiClient resourceGroupsTaggingApiClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of())
			.tags(Set.of("governance", "tagging", "resource-groups", "aws"))
			.build();

	public TagEditorResourceCollector()
	{
		log.debug("TagEditorResourceCollector created");
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
		log.debug("TagEditorResourceCollector.collectEntities called");

		if (this.resourceGroupsTaggingApiClient == null)
		{
			this.resourceGroupsTaggingApiClient =
				AwsClientFactory.getInstance().getResourceGroupsTaggingApiClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			GetResourcesRequest.Builder requestBuilder = GetResourcesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.resourcesPerPage(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("TagEditorResourceCollector resuming from paginationToken: {}", token);
				requestBuilder.paginationToken(token);
			});

			GetResourcesResponse response = this.resourceGroupsTaggingApiClient.getResources(requestBuilder.build());
			List<ResourceTagMapping> mappings       = response.resourceTagMappingList();
			String                   paginationToken = response.paginationToken();

			log.debug("TagEditorResourceCollector received {} resources, paginationToken={}",
				mappings != null ? mappings.size() : 0, paginationToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (mappings != null)
			{
				for (ResourceTagMapping mapping : mappings)
				{
					if (mapping == null) continue;

					String resourceArn = mapping.resourceARN();
					if (resourceArn == null || resourceArn.isBlank()) continue;

					// Derive a short name from the ARN — last segment after '/' or ':'
					String resourceName = resourceArn;
					int slashIdx = resourceArn.lastIndexOf('/');
					int colonIdx = resourceArn.lastIndexOf(':');
					int sepIdx   = Math.max(slashIdx, colonIdx);
					if (sepIdx >= 0 && sepIdx < resourceArn.length() - 1)
					{
						resourceName = resourceArn.substring(sepIdx + 1);
					}

					Map<String, String> tagMap = mapping.tags() != null
						? mapping.tags().stream()
							.filter(t -> t.key() != null)
							.collect(Collectors.toMap(
								t -> t.key(),
								t -> t.value() != null ? t.value() : "",
								(a, b) -> a))
						: Map.of();

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("resourceArn",  resourceArn);
					attributes.put("resourceName", resourceName);
					attributes.put("accountId",    accountId);
					attributes.put("region",       region);
					attributes.put("tags",         tagMap);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(resourceArn)
							.qualifiedResourceName(resourceArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(resourceName)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(mapping)
						.collectedAt(now)
						.build();

					entities.add(entity);
				}
			}

			// Treat empty paginationToken as end of results
			String finalPaginationToken = (paginationToken != null && !paginationToken.isBlank())
				? paginationToken : null;

			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken()
				{
					return Optional.ofNullable(finalPaginationToken);
				}
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (ResourceGroupsTaggingApiException e)
		{
			log.error("TagEditorResourceCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect tagged resources via Tag Editor API", e);
		}
		catch (Exception e)
		{
			log.error("TagEditorResourceCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting tagged resources", e);
		}
	}
}
