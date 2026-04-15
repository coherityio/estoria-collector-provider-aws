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
import software.amazon.awssdk.services.elasticache.model.CacheCluster;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersResponse;
import software.amazon.awssdk.services.elasticache.model.ElastiCacheException;

/**
 * Collects ElastiCache clusters via the ElastiCache DescribeCacheClusters API.
 */
@Slf4j
public class ElastiCacheClusterCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "ElastiCacheCluster";

    private ElastiCacheClient elastiCacheClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "elasticache", "cache", "aws"))
            .build();

    public ElastiCacheClusterCollector()
    {
        log.debug("ElastiCacheClusterCollector created");
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
        log.debug("ElastiCacheClusterCollector.collectEntities called");

        if (this.elastiCacheClient == null)
        {
            this.elastiCacheClient = AwsClientFactory.getInstance().getElastiCacheClient(providerContext);
        }

        try
        {
            DescribeCacheClustersRequest.Builder requestBuilder = DescribeCacheClustersRequest.builder()
                .showCacheNodeInfo(true);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("ElastiCacheClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeCacheClustersResponse response = this.elastiCacheClient.describeCacheClusters(requestBuilder.build());
            List<CacheCluster> clusters = response.cacheClusters();
            String nextMarker = response.marker();

            log.debug("ElastiCacheClusterCollector received {} clusters, nextMarker={}",
                clusters != null ? clusters.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (CacheCluster cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterId = cluster.cacheClusterId();
                    String arn       = cluster.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("cacheClusterId", clusterId);
                    attributes.put("arn", arn);
                    attributes.put("engine", cluster.engine());
                    attributes.put("engineVersion", cluster.engineVersion());
                    attributes.put("cacheClusterStatus", cluster.cacheClusterStatus());
                    attributes.put("cacheNodeType", cluster.cacheNodeType());
                    attributes.put("numCacheNodes", cluster.numCacheNodes());
                    attributes.put("preferredAvailabilityZone", cluster.preferredAvailabilityZone());
                    attributes.put("cacheSubnetGroupName", cluster.cacheSubnetGroupName());
                    attributes.put("replicationGroupId", cluster.replicationGroupId());
                    attributes.put("authTokenEnabled", cluster.authTokenEnabled());
                    attributes.put("atRestEncryptionEnabled", cluster.atRestEncryptionEnabled());
                    attributes.put("transitEncryptionEnabled", cluster.transitEncryptionEnabled());
                    attributes.put("cacheClusterCreateTime",
                        cluster.cacheClusterCreateTime() != null ? cluster.cacheClusterCreateTime().toString() : null);

                    String endpointAddress = null;
                    Integer endpointPort = null;
                    if (cluster.configurationEndpoint() != null)
                    {
                        endpointAddress = cluster.configurationEndpoint().address();
                        endpointPort    = cluster.configurationEndpoint().port();
                    }
                    attributes.put("endpointAddress", endpointAddress);
                    attributes.put("endpointPort", endpointPort);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : clusterId)
                            .qualifiedResourceName(arn != null ? arn : clusterId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(clusterId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(cluster)
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
            log.error("ElastiCacheClusterCollector ElastiCache error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect ElastiCache clusters", e);
        }
        catch (Exception e)
        {
            log.error("ElastiCacheClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting ElastiCache clusters", e);
        }
    }
}
