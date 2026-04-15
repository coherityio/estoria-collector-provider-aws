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
import software.amazon.awssdk.services.emr.model.InstanceGroup;
import software.amazon.awssdk.services.emr.model.ListClustersRequest;
import software.amazon.awssdk.services.emr.model.ListClustersResponse;
import software.amazon.awssdk.services.emr.model.ListInstanceGroupsRequest;
import software.amazon.awssdk.services.emr.model.ListInstanceGroupsResponse;

/**
 * Collects EMR instance groups by enumerating active clusters and calling
 * ListInstanceGroups per cluster.
 */
@Slf4j
public class EmrInstanceGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EmrInstanceGroup";

    private EmrClient emrClient;

    public EmrInstanceGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(EmrClusterCollector.ENTITY_TYPE), Set.of("integration", "emr", "analytics", "aws")).build());
        log.debug("EmrInstanceGroupCollector created");
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
        log.debug("EmrInstanceGroupCollector.collectEntities called");

        if (this.emrClient == null)
        {
            this.emrClient = AwsClientFactory.getInstance().getEmrClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // Step 1: enumerate active clusters
            List<ClusterSummary> allClusters = new ArrayList<>();
            String clusterMarker = null;
            do
            {
                ListClustersRequest.Builder req = ListClustersRequest.builder()
                    .clusterStates(
                        ClusterState.STARTING,
                        ClusterState.BOOTSTRAPPING,
                        ClusterState.RUNNING,
                        ClusterState.WAITING,
                        ClusterState.TERMINATING);
                if (clusterMarker != null) req.marker(clusterMarker);
                ListClustersResponse resp = this.emrClient.listClusters(req.build());
                if (resp.clusters() != null) allClusters.addAll(resp.clusters());
                clusterMarker = resp.marker();
            }
            while (clusterMarker != null && !clusterMarker.isBlank());

            log.debug("EmrInstanceGroupCollector found {} active clusters to inspect", allClusters.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            // Step 2: get instance groups per cluster
            for (ClusterSummary cluster : allClusters)
            {
                if (cluster == null) continue;

                String clusterId  = cluster.id();
                String clusterArn = cluster.clusterArn() != null
                    ? cluster.clusterArn()
                    : "arn:aws:elasticmapreduce:" + region + ":" + accountId + ":cluster/" + clusterId;

                String igMarker = null;
                do
                {
                    ListInstanceGroupsRequest.Builder igReq = ListInstanceGroupsRequest.builder()
                        .clusterId(clusterId);
                    if (igMarker != null) igReq.marker(igMarker);

                    ListInstanceGroupsResponse igResp = this.emrClient.listInstanceGroups(igReq.build());
                    igMarker = igResp.marker();

                    List<InstanceGroup> groups = igResp.instanceGroups();
                    if (groups != null)
                    {
                        for (InstanceGroup group : groups)
                        {
                            if (group == null) continue;

                            String groupId = group.id();
                            // Synthetic ARN — EMR instance groups don't have native ARNs
                            String groupArn = clusterArn + "/instancegroup/" + groupId;

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("instanceGroupId",    groupId);
                            attributes.put("instanceGroupArn",   groupArn);
                            attributes.put("instanceGroupName",  group.name());
                            attributes.put("clusterId",          clusterId);
                            attributes.put("clusterArn",         clusterArn);
                            attributes.put("instanceGroupType",
                                group.instanceGroupType() != null ? group.instanceGroupType().toString() : null);
                            attributes.put("instanceType",       group.instanceType());
                            attributes.put("requestedInstanceCount", group.requestedInstanceCount());
                            attributes.put("runningInstanceCount",   group.runningInstanceCount());
                            attributes.put("state",
                                group.status() != null && group.status().state() != null
                                    ? group.status().state().toString() : null);
                            attributes.put("market",
                                group.market() != null ? group.market().toString() : null);
                            attributes.put("bidPrice",           group.bidPrice());
                            attributes.put("accountId",          accountId);
                            attributes.put("region",             region);

                            String groupName = group.name() != null ? group.name()
                                : (group.instanceGroupType() != null ? group.instanceGroupType().toString() : groupId);

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(groupArn)
                                    .qualifiedResourceName(groupArn)
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
                }
                while (igMarker != null && !igMarker.isBlank());
            }

            log.debug("EmrInstanceGroupCollector collected {} instance groups across {} clusters",
                entities.size(), allClusters.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());
            metadataValues.put("clusterCount", allClusters.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (EmrException e)
        {
            log.error("EmrInstanceGroupCollector EMR error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EMR instance groups", e);
        }
        catch (Exception e)
        {
            log.error("EmrInstanceGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EMR instance groups", e);
        }
    }
}
