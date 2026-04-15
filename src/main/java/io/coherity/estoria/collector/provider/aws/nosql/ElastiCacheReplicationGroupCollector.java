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
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsRequest;
import software.amazon.awssdk.services.elasticache.model.DescribeReplicationGroupsResponse;
import software.amazon.awssdk.services.elasticache.model.ElastiCacheException;
import software.amazon.awssdk.services.elasticache.model.ReplicationGroup;

/**
 * Collects ElastiCache replication groups via the ElastiCache DescribeReplicationGroups API.
 */
@Slf4j
public class ElastiCacheReplicationGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "ElastiCacheReplicationGroup";

    private ElastiCacheClient elastiCacheClient;

    public ElastiCacheReplicationGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "elasticache", "cache", "redis", "aws")).build());
        log.debug("ElastiCacheReplicationGroupCollector created");
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
        log.debug("ElastiCacheReplicationGroupCollector.collectEntities called");

        if (this.elastiCacheClient == null)
        {
            this.elastiCacheClient = AwsClientFactory.getInstance().getElastiCacheClient(providerContext);
        }

        try
        {
            DescribeReplicationGroupsRequest.Builder requestBuilder = DescribeReplicationGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("ElastiCacheReplicationGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeReplicationGroupsResponse response =
                this.elastiCacheClient.describeReplicationGroups(requestBuilder.build());
            List<ReplicationGroup> groups = response.replicationGroups();
            String nextMarker = response.marker();

            log.debug("ElastiCacheReplicationGroupCollector received {} groups, nextMarker={}",
                groups != null ? groups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (groups != null)
            {
                for (ReplicationGroup group : groups)
                {
                    if (group == null) continue;

                    String groupId = group.replicationGroupId();
                    String arn     = group.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("replicationGroupId", groupId);
                    attributes.put("arn", arn);
                    attributes.put("description", group.description());
                    attributes.put("status", group.status());
                    attributes.put("cacheNodeType", group.cacheNodeType());
                    attributes.put("automaticFailover", group.automaticFailover() != null
                        ? group.automaticFailover().toString() : null);
                    attributes.put("multiAZ", group.multiAZ() != null ? group.multiAZ().toString() : null);
                    attributes.put("atRestEncryptionEnabled", group.atRestEncryptionEnabled());
                    attributes.put("transitEncryptionEnabled", group.transitEncryptionEnabled());
                    attributes.put("authTokenEnabled", group.authTokenEnabled());
                    attributes.put("clusterEnabled", group.clusterEnabled());
                    attributes.put("memberClusters", group.memberClusters());
                    attributes.put("snapshotRetentionLimit", group.snapshotRetentionLimit());
                    attributes.put("snapshotWindow", group.snapshotWindow());

                    String primaryEndpointAddress = null;
                    if (group.nodeGroups() != null && !group.nodeGroups().isEmpty() &&
                        group.nodeGroups().get(0).primaryEndpoint() != null)
                    {
                        primaryEndpointAddress = group.nodeGroups().get(0).primaryEndpoint().address();
                    }
                    attributes.put("primaryEndpointAddress", primaryEndpointAddress);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : groupId)
                            .qualifiedResourceName(arn != null ? arn : groupId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(groupId)
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
            log.error("ElastiCacheReplicationGroupCollector ElastiCache error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect ElastiCache replication groups", e);
        }
        catch (Exception e)
        {
            log.error("ElastiCacheReplicationGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting ElastiCache replication groups", e);
        }
    }
}
