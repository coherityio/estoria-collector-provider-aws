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
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.kafka.model.ListClustersRequest;
import software.amazon.awssdk.services.kafka.model.ListClustersResponse;

/**
 * Collects Amazon MSK (Managed Streaming for Apache Kafka) clusters via the Kafka ListClusters API.
 */
@Slf4j
public class KafkaMskClusterCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "KafkaMskCluster";

    private KafkaClient kafkaClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("kafka", "msk", "streaming", "messaging", "aws"))
            .build();

    public KafkaMskClusterCollector()
    {
        log.debug("KafkaMskClusterCollector created");
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
        log.debug("KafkaMskClusterCollector.collectEntities called");

        if (this.kafkaClient == null)
        {
            this.kafkaClient = AwsClientFactory.getInstance().getKafkaClient(providerContext);
        }

        try
        {
            ListClustersRequest.Builder requestBuilder = ListClustersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("KafkaMskClusterCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListClustersResponse response = this.kafkaClient.listClusters(requestBuilder.build());
            List<ClusterInfo> clusters = response.clusterInfoList();
            String nextToken = response.nextToken();

            log.debug("KafkaMskClusterCollector received {} clusters, nextToken={}",
                clusters != null ? clusters.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (clusters != null)
            {
                for (ClusterInfo cluster : clusters)
                {
                    if (cluster == null) continue;

                    String clusterName = cluster.clusterName();
                    String clusterArn  = cluster.clusterArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("clusterName", clusterName);
                    attributes.put("clusterArn", clusterArn);
                    attributes.put("state", cluster.state() != null ? cluster.state().toString() : null);
                    attributes.put("clusterType", "PROVISIONED");
                    attributes.put("creationTime",
                        cluster.creationTime() != null ? cluster.creationTime().toString() : null);
                    attributes.put("currentBrokerSoftwareInfo",
                        cluster.currentBrokerSoftwareInfo() != null
                            ? cluster.currentBrokerSoftwareInfo().kafkaVersion() : null);
                    attributes.put("numberOfBrokerNodes",
                        cluster.brokerNodeGroupInfo() != null
                            ? cluster.numberOfBrokerNodes() : null);
                    attributes.put("brokerInstanceType",
                        cluster.brokerNodeGroupInfo() != null
                            ? cluster.brokerNodeGroupInfo().instanceType() : null);
                    attributes.put("encryptionInTransitClientBroker",
                        cluster.encryptionInfo() != null
                            && cluster.encryptionInfo().encryptionInTransit() != null
                            ? cluster.encryptionInfo().encryptionInTransit().clientBroker().toString()
                            : null);
                    attributes.put("encryptionAtRestDataVolumeKMSKeyId",
                        cluster.encryptionInfo() != null
                            && cluster.encryptionInfo().encryptionAtRest() != null
                            ? cluster.encryptionInfo().encryptionAtRest().dataVolumeKMSKeyId()
                            : null);
                    attributes.put("tags", cluster.tags() != null ? cluster.tags() : new HashMap<>());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(clusterArn)
                            .qualifiedResourceName(clusterArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(clusterName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(cluster)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (KafkaException e)
        {
            log.error("KafkaMskClusterCollector Kafka error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect MSK clusters", e);
        }
        catch (Exception e)
        {
            log.error("KafkaMskClusterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting MSK clusters", e);
        }
    }
}
