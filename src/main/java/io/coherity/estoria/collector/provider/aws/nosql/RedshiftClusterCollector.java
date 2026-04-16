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
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.model.Cluster;
import software.amazon.awssdk.services.redshift.model.DescribeClustersRequest;
import software.amazon.awssdk.services.redshift.model.DescribeClustersResponse;
import software.amazon.awssdk.services.redshift.model.RedshiftException;
import software.amazon.awssdk.services.redshift.model.Tag;

/**
 * Collects Redshift clusters via the Redshift DescribeClusters API.
 */
@Slf4j
public class RedshiftClusterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RedshiftCluster";


    public RedshiftClusterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "redshift", "datawarehouse", "aws")).build());
        log.debug("RedshiftClusterCollector created");
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
        log.debug("RedshiftClusterCollector.collectEntities called");

        RedshiftClient redshiftClient = AwsClientFactory.getInstance().getRedshiftClient(providerContext);

        try
        {
            DescribeClustersRequest.Builder requestBuilder = DescribeClustersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RedshiftClusterCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeClustersResponse response = redshiftClient.describeClusters(requestBuilder.build());
            List<Cluster> clusters = response.clusters();
            String nextMarker = response.marker();

            log.debug("RedshiftClusterCollector received {} clusters, nextMarker={}",
                clusters != null ? clusters.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (Cluster cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterIdentifier = cluster.clusterIdentifier();
                    // Redshift ARN format: arn:aws:redshift:region:account:cluster:id
                    String arn = cluster.clusterNamespaceArn();

                    Map<String, String> tags = cluster.tags() == null ? new HashMap<>()
                        : cluster.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("clusterIdentifier", clusterIdentifier);
                    attributes.put("clusterNamespaceArn", arn);
                    attributes.put("nodeType", cluster.nodeType());
                    attributes.put("clusterStatus", cluster.clusterStatus());
                    attributes.put("masterUsername", cluster.masterUsername());
                    attributes.put("dbName", cluster.dbName());
                    attributes.put("numberOfNodes", cluster.numberOfNodes());
                    attributes.put("availabilityZone", cluster.availabilityZone());
                    attributes.put("port", cluster.endpoint() != null ? cluster.endpoint().port() : null);
                    attributes.put("endpointAddress", cluster.endpoint() != null ? cluster.endpoint().address() : null);
                    attributes.put("encrypted", cluster.encrypted());
                    attributes.put("publiclyAccessible", cluster.publiclyAccessible());
                    attributes.put("enhancedVpcRouting", cluster.enhancedVpcRouting());
                    attributes.put("clusterSubnetGroupName", cluster.clusterSubnetGroupName());
                    attributes.put("vpcId", cluster.vpcId());
                    attributes.put("kmsKeyId", cluster.kmsKeyId());
                    attributes.put("clusterCreateTime",
                        cluster.clusterCreateTime() != null ? cluster.clusterCreateTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : clusterIdentifier)
                            .qualifiedResourceName(arn != null ? arn : clusterIdentifier)
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
        catch (RedshiftException e)
        {
            log.error("RedshiftClusterCollector Redshift error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Redshift clusters", e);
        }
        catch (Exception e)
        {
            log.error("RedshiftClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Redshift clusters", e);
        }
    }
}
