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
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.awssdk.services.rds.model.RdsException;

/**
 * Collects RDS global database clusters via the RDS DescribeGlobalClusters API.
 *
 * <p>Global clusters are global (not regional) resources, but the RDS API endpoint
 * used here is regional. They are placed in ACCOUNT_GLOBAL containment scope
 * because their membership spans regions.
 */
@Slf4j
public class RdsGlobalClusterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsGlobalCluster";

    private RdsClient rdsClient;

    public RdsGlobalClusterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "aurora", "global", "aws")).build());
        log.debug("RdsGlobalClusterCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("RdsGlobalClusterCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeGlobalClustersRequest.Builder requestBuilder = DescribeGlobalClustersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsGlobalClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeGlobalClustersResponse response = this.rdsClient.describeGlobalClusters(requestBuilder.build());
            List<GlobalCluster> globalClusters = response.globalClusters();
            String nextMarker = response.marker();

            log.debug("RdsGlobalClusterCollector received {} global clusters, nextMarker={}",
                globalClusters != null ? globalClusters.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (globalClusters != null)
            {
                for (GlobalCluster cluster : globalClusters)
                {
                    if (cluster == null) continue;

                    String globalClusterIdentifier = cluster.globalClusterIdentifier();
                    String arn = cluster.globalClusterArn();
                    String resourceId = cluster.globalClusterResourceId();

                    List<String> memberArns = cluster.globalClusterMembers() == null ? List.of()
                        : cluster.globalClusterMembers().stream()
                            .map(GlobalClusterMember::dbClusterArn)
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("globalClusterIdentifier", globalClusterIdentifier);
                    attributes.put("globalClusterArn", arn);
                    attributes.put("globalClusterResourceId", resourceId);
                    attributes.put("engine", cluster.engine());
                    attributes.put("engineVersion", cluster.engineVersion());
                    attributes.put("status", cluster.status());
                    attributes.put("storageEncrypted", cluster.storageEncrypted());
                    attributes.put("deletionProtection", cluster.deletionProtection());
                    attributes.put("databaseName", cluster.databaseName());
                    attributes.put("failoverState", cluster.failoverState() != null
                        ? cluster.failoverState().statusAsString() : null);
                    attributes.put("memberClusterArns", memberArns);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : globalClusterIdentifier)
                            .qualifiedResourceName(arn != null ? arn : globalClusterIdentifier)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(globalClusterIdentifier)
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
            log.error("RdsGlobalClusterCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS global clusters", e);
        }
        catch (Exception e)
        {
            log.error("RdsGlobalClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS global clusters", e);
        }
    }
}
