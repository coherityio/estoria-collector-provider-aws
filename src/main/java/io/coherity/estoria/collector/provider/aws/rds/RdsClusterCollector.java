package io.coherity.estoria.collector.provider.aws.rds;

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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;

/**
 * Collects Aurora/RDS DB clusters via the RDS DescribeDBClusters API.
 */
@Slf4j
public class RdsClusterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsCluster";

    private RdsClient rdsClient;

    public RdsClusterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "aurora", "aws")).build());
        log.debug("RdsClusterCollector created");
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
        log.debug("RdsClusterCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeDbClustersRequest.Builder requestBuilder = DescribeDbClustersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbClustersResponse response = this.rdsClient.describeDBClusters(requestBuilder.build());
            List<DBCluster> clusters = response.dbClusters();
            String nextMarker = response.marker();

            log.debug("RdsClusterCollector received {} clusters, nextMarker={}",
                clusters != null ? clusters.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (DBCluster cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterIdentifier = cluster.dbClusterIdentifier();
                    String arn = cluster.dbClusterArn();

                    Map<String, String> tags = cluster.tagList() == null ? new HashMap<>()
                        : cluster.tagList().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", clusterIdentifier);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbClusterIdentifier", clusterIdentifier);
                    attributes.put("dbClusterArn", arn);
                    attributes.put("engine", cluster.engine());
                    attributes.put("engineVersion", cluster.engineVersion());
                    attributes.put("engineMode", cluster.engineMode());
                    attributes.put("status", cluster.status());
                    attributes.put("masterUsername", cluster.masterUsername());
                    attributes.put("databaseName", cluster.databaseName());
                    attributes.put("availabilityZones", cluster.availabilityZones());
                    attributes.put("multiAZ", cluster.multiAZ());
                    attributes.put("port", cluster.port());
                    attributes.put("endpoint", cluster.endpoint());
                    attributes.put("readerEndpoint", cluster.readerEndpoint());
                    attributes.put("storageEncrypted", cluster.storageEncrypted());
                    attributes.put("kmsKeyId", cluster.kmsKeyId());
                    attributes.put("iamDatabaseAuthenticationEnabled", cluster.iamDatabaseAuthenticationEnabled());
                    attributes.put("deletionProtection", cluster.deletionProtection());
                    attributes.put("globalWriteForwardingStatus", cluster.globalWriteForwardingStatusAsString());
                    attributes.put("clusterCreateTime",
                        cluster.clusterCreateTime() != null ? cluster.clusterCreateTime().toString() : null);
                    attributes.put("earliestRestorableTime",
                        cluster.earliestRestorableTime() != null ? cluster.earliestRestorableTime().toString() : null);
                    attributes.put("latestRestorableTime",
                        cluster.latestRestorableTime() != null ? cluster.latestRestorableTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : clusterIdentifier)
                            .qualifiedResourceName(arn != null ? arn : clusterIdentifier)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
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
        catch (RdsException e)
        {
            log.error("RdsClusterCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS clusters", e);
        }
        catch (Exception e)
        {
            log.error("RdsClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS clusters", e);
        }
    }
}
