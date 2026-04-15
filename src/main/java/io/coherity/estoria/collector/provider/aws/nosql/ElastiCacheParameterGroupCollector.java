package io.coherity.estoria.collector.provider.aws.nosql;

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
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CacheParameterGroup;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheParameterGroupsRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheParameterGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.ElastiCacheException;

/**
 * Collects ElastiCache parameter groups via the ElastiCache DescribeCacheParameterGroups API.
 */
@Slf4j
public class ElastiCacheParameterGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "ElastiCacheParameterGroup";

    private ElastiCacheClient elastiCacheClient;

    public ElastiCacheParameterGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "elasticache", "configuration", "aws")).build());
        log.debug("ElastiCacheParameterGroupCollector created");
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
        log.debug("ElastiCacheParameterGroupCollector.collectEntities called");

        if (this.elastiCacheClient == null)
        {
            this.elastiCacheClient = AwsClientFactory.getInstance().getElastiCacheClient(providerContext);
        }

        try
        {
            DescribeCacheParameterGroupsRequest.Builder requestBuilder =
                DescribeCacheParameterGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("ElastiCacheParameterGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeCacheParameterGroupsResponse response =
                this.elastiCacheClient.describeCacheParameterGroups(requestBuilder.build());
            List<CacheParameterGroup> paramGroups = response.cacheParameterGroups();
            String nextMarker = response.marker();

            log.debug("ElastiCacheParameterGroupCollector received {} parameter groups, nextMarker={}",
                paramGroups != null ? paramGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (paramGroups != null)
            {
                for (CacheParameterGroup group : paramGroups)
                {
                    if (group == null) continue;

                    String groupName = group.cacheParameterGroupName();
                    String arn       = group.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("cacheParameterGroupName", groupName);
                    attributes.put("arn", arn);
                    attributes.put("cacheParameterGroupFamily", group.cacheParameterGroupFamily());
                    attributes.put("description", group.description());
                    attributes.put("isGlobal", group.isGlobal());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : groupName)
                            .qualifiedResourceName(arn != null ? arn : groupName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(groupName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(group)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextMarker = nextMarker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextMarker).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (ElastiCacheException e)
        {
            log.error("ElastiCacheParameterGroupCollector ElastiCache error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect ElastiCache parameter groups", e);
        }
        catch (Exception e)
        {
            log.error("ElastiCacheParameterGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting ElastiCache parameter groups", e);
        }
    }
}
