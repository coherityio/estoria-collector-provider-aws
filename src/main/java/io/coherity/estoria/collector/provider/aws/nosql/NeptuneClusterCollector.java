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
import software.amazon.awssdk.services.neptune.NeptuneClient;
import software.amazon.awssdk.services.neptune.model.DBCluster;
import software.amazon.awssdk.services.neptune.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.neptune.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.neptune.model.Filter;
import software.amazon.awssdk.services.neptune.model.NeptuneException;
import software.amazon.awssdk.services.neptune.model.Tag;

/**
 * Collects Neptune DB clusters via the Neptune DescribeDBClusters API.
 */
@Slf4j
public class NeptuneClusterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "NeptuneCluster";

    private NeptuneClient neptuneClient;

    public NeptuneClusterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "neptune", "graph", "aws")).build());
        log.debug("NeptuneClusterCollector created");
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
        log.debug("NeptuneClusterCollector.collectEntities called");

        if (this.neptuneClient == null)
        {
            this.neptuneClient = AwsClientFactory.getInstance().getNeptuneClient(providerContext);
        }

        try
        {
            DescribeDbClustersRequest.Builder requestBuilder = DescribeDbClustersRequest.builder()
                .filters(List.of(Filter.builder().name("engine").values("neptune").build()));

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("NeptuneClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbClustersResponse response = this.neptuneClient.describeDBClusters(requestBuilder.build());
            List<DBCluster> clusters = response.dbClusters();
            String nextMarker = response.marker();

            log.debug("NeptuneClusterCollector received {} clusters, nextMarker={}",
                clusters != null ? clusters.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (DBCluster cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterIdentifier = cluster.dbClusterIdentifier();
                    String arn               = cluster.dbClusterArn();

                    Map<String, String> tags = new HashMap<>();
                    if (arn != null && !arn.isBlank())
                    {
                        tags = this.neptuneClient.listTagsForResource(r -> r.resourceName(arn))
                            .tagList()
                            .stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
                    }                    

                    List<String> memberIds = cluster.dbClusterMembers() == null ? List.of()
                        : cluster.dbClusterMembers().stream()
                            .map(m -> m.dbInstanceIdentifier())
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbClusterIdentifier", clusterIdentifier);
                    attributes.put("dbClusterArn", arn);
                    attributes.put("engine", cluster.engine());
                    attributes.put("engineVersion", cluster.engineVersion());
                    attributes.put("status", cluster.status());
                    attributes.put("endpoint", cluster.endpoint());
                    attributes.put("readerEndpoint", cluster.readerEndpoint());
                    attributes.put("port", cluster.port());
                    attributes.put("dbSubnetGroup", cluster.dbSubnetGroup());
                    attributes.put("availabilityZones", cluster.availabilityZones());
                    attributes.put("multiAZ", cluster.multiAZ());
                    attributes.put("storageEncrypted", cluster.storageEncrypted());
                    attributes.put("kmsKeyId", cluster.kmsKeyId());
                    attributes.put("iamDatabaseAuthenticationEnabled", cluster.iamDatabaseAuthenticationEnabled());
                    attributes.put("deletionProtection", cluster.deletionProtection());
                    attributes.put("clusterMembers", memberIds);
                    attributes.put("clusterCreateTime",
                        cluster.clusterCreateTime() != null ? cluster.clusterCreateTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(clusterIdentifier)
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
        catch (NeptuneException e)
        {
            log.error("NeptuneClusterCollector Neptune error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Neptune clusters", e);
        }
        catch (Exception e)
        {
            log.error("NeptuneClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Neptune clusters", e);
        }
    }
}
