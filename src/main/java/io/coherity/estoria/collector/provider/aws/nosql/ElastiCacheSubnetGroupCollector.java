package io.coherity.estoria.collector.provider.aws.nosql;

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
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.elasticache.model.CacheSubnetGroup;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheSubnetGroupsRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheSubnetGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.ElastiCacheException;

/**
 * Collects ElastiCache subnet groups via the ElastiCache DescribeCacheSubnetGroups API.
 */
@Slf4j
public class ElastiCacheSubnetGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "ElastiCacheSubnetGroup";


    public ElastiCacheSubnetGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "elasticache", "networking", "aws")).build());
        log.debug("ElastiCacheSubnetGroupCollector created");
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
        log.debug("ElastiCacheSubnetGroupCollector.collectEntities called");

        ElastiCacheClient elastiCacheClient = AwsClientFactory.getInstance().getElastiCacheClient(providerContext);

        try
        {
            DescribeCacheSubnetGroupsRequest.Builder requestBuilder = DescribeCacheSubnetGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("ElastiCacheSubnetGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeCacheSubnetGroupsResponse response =
                elastiCacheClient.describeCacheSubnetGroups(requestBuilder.build());
            List<CacheSubnetGroup> subnetGroups = response.cacheSubnetGroups();
            String nextMarker = response.marker();

            log.debug("ElastiCacheSubnetGroupCollector received {} subnet groups, nextMarker={}",
                subnetGroups != null ? subnetGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subnetGroups != null)
            {
                for (CacheSubnetGroup group : subnetGroups)
                {
                    if (group == null) continue;

                    String groupName = group.cacheSubnetGroupName();
                    String arn       = group.arn();

                    List<String> subnetIds = group.subnets() == null ? List.of()
                        : group.subnets().stream()
                            .map(s -> s.subnetIdentifier())
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("cacheSubnetGroupName", groupName);
                    attributes.put("arn", arn);
                    attributes.put("description", group.cacheSubnetGroupDescription());
                    attributes.put("vpcId", group.vpcId());
                    attributes.put("subnetIds", subnetIds);

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
            log.error("ElastiCacheSubnetGroupCollector ElastiCache error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect ElastiCache subnet groups", e);
        }
        catch (Exception e)
        {
            log.error("ElastiCacheSubnetGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting ElastiCache subnet groups", e);
        }
    }
}
