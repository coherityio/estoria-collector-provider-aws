package io.coherity.estoria.collector.provider.aws.integration;

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
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ClusterState;
import software.amazon.awssdk.services.emr.model.ClusterSummary;
import software.amazon.awssdk.services.emr.model.EmrException;
import software.amazon.awssdk.services.emr.model.ListClustersRequest;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;

/**
 * Collects EMR clusters via the EMR ListClusters API.
 * Collects clusters in all non-terminal states by default.
 */
@Slf4j
public class EmrClusterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EmrCluster";

    private EmrClient emrClient;

    public EmrClusterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "emr", "analytics", "aws")).build());
        log.debug("EmrClusterCollector created");
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
        log.debug("EmrClusterCollector.collectEntities called");

        if (this.emrClient == null)
        {
            this.emrClient = AwsClientFactory.getInstance().getEmrClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListClustersRequest.Builder requestBuilder = ListClustersRequest.builder()
                .clusterStates(
                    ClusterState.STARTING,
                    ClusterState.BOOTSTRAPPING,
                    ClusterState.RUNNING,
                    ClusterState.WAITING,
                    ClusterState.TERMINATING,
                    ClusterState.TERMINATED,
                    ClusterState.TERMINATED_WITH_ERRORS);

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EmrClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListClustersResponse response = this.emrClient.listClusters(requestBuilder.build());
            List<ClusterSummary> clusters  = response.clusters();
            String               marker    = response.marker();

            log.debug("EmrClusterCollector received {} clusters, marker={}",
                clusters != null ? clusters.size() : 0, marker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (ClusterSummary cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterId   = cluster.id();
                    String clusterName = cluster.name();
                    // EMR cluster ARN: arn:aws:elasticmapreduce:<region>:<accountId>:cluster/<id>
                    String clusterArn  = cluster.clusterArn() != null
                        ? cluster.clusterArn()
                        : "arn:aws:elasticmapreduce:" + region + ":" + accountId + ":cluster/" + clusterId;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("clusterId",       clusterId);
                    attributes.put("clusterName",     clusterName);
                    attributes.put("clusterArn",      clusterArn);
                    attributes.put("state",
                        cluster.status() != null && cluster.status().state() != null
                            ? cluster.status().state().toString() : null);
                    attributes.put("stateChangeReason",
                        cluster.status() != null && cluster.status().stateChangeReason() != null
                            ? cluster.status().stateChangeReason().message() : null);
                    attributes.put("createdDateTime",
                        cluster.status() != null && cluster.status().timeline() != null
                            && cluster.status().timeline().creationDateTime() != null
                            ? cluster.status().timeline().creationDateTime().toString() : null);
                    attributes.put("normalizedInstanceHours", cluster.normalizedInstanceHours());
                    attributes.put("outpostArn",              cluster.outpostArn());
                    attributes.put("accountId",               accountId);
                    attributes.put("region",                  region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(clusterArn)
                            .qualifiedResourceName(clusterArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(clusterName != null ? clusterName : clusterId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(cluster)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalMarker = marker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalMarker).filter(m -> !m.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (EmrException e)
        {
            log.error("EmrClusterCollector EMR error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EMR clusters", e);
        }
        catch (Exception e)
        {
            log.error("EmrClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EMR clusters", e);
        }
    }
}
