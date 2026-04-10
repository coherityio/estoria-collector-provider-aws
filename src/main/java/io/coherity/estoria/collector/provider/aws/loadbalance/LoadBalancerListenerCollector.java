package io.coherity.estoria.collector.provider.aws.loadbalance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2Client;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeListenersRequest;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeListenersResponse;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.ElasticLoadBalancingV2Exception;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.Listener;

/**
 * Collects all ELBv2 listeners across all load balancers via DescribeListeners (no LB ARN filter).
 */
@Slf4j
public class LoadBalancerListenerCollector implements Collector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "LoadBalancerListener";

    private ElasticLoadBalancingV2Client elbV2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "listener", "aws"))
            .build();

    public LoadBalancerListenerCollector()
    {
        log.debug("LoadBalancerListenerCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("LoadBalancerListenerCollector.collect called");

        if (this.elbV2Client == null)
        {
            this.elbV2Client = AwsClientFactory.getInstance().getElbV2Client(providerContext);
        }

        try
        {
            DescribeListenersRequest.Builder requestBuilder = DescribeListenersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.pageSize(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("LoadBalancerListenerCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeListenersResponse response = this.elbV2Client.describeListeners(requestBuilder.build());
            List<Listener> listeners = response.listeners();
            String nextMarker = response.nextMarker();

            log.debug("LoadBalancerListenerCollector received {} listeners, nextMarker={}", 
                listeners != null ? listeners.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (listeners != null)
            {
                for (Listener listener : listeners)
                {
                    if (listener == null) continue;

                    String arn = listener.listenerArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("listenerArn", arn);
                    attributes.put("loadBalancerArn", listener.loadBalancerArn());
                    attributes.put("port", listener.port());
                    attributes.put("protocol", listener.protocolAsString());
                    attributes.put("sslPolicy", listener.sslPolicy());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(listener.protocolAsString() + ":" + listener.port())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(listener)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextToken = nextMarker;
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
        catch (ElasticLoadBalancingV2Exception e)
        {
            log.error("LoadBalancerListenerCollector ELBv2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect load balancer listeners", e);
        }
        catch (Exception e)
        {
            log.error("LoadBalancerListenerCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting load balancer listeners", e);
        }
    }
}
