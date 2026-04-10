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
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetGroupsResponse;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.ElasticLoadBalancingV2Exception;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.TargetGroup;

/**
 * Collects ELBv2 target groups via DescribeTargetGroups.
 */
@Slf4j
public class TargetGroupCollector implements Collector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "TargetGroup";

    private ElasticLoadBalancingV2Client elbV2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "target-group", "aws"))
            .build();

    public TargetGroupCollector()
    {
        log.debug("TargetGroupCollector created");
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
        log.debug("TargetGroupCollector.collect called");

        if (this.elbV2Client == null)
        {
            this.elbV2Client = AwsClientFactory.getInstance().getElbV2Client(providerContext);
        }

        try
        {
            DescribeTargetGroupsRequest.Builder requestBuilder = DescribeTargetGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.pageSize(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("TargetGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeTargetGroupsResponse response = this.elbV2Client.describeTargetGroups(requestBuilder.build());
            List<TargetGroup> targetGroups = response.targetGroups();
            String nextMarker = response.nextMarker();

            log.debug("TargetGroupCollector received {} target groups, nextMarker={}",
                targetGroups != null ? targetGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (targetGroups != null)
            {
                for (TargetGroup tg : targetGroups)
                {
                    if (tg == null) continue;

                    String arn = tg.targetGroupArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("targetGroupArn", arn);
                    attributes.put("targetGroupName", tg.targetGroupName());
                    attributes.put("protocol", tg.protocolAsString());
                    attributes.put("port", tg.port());
                    attributes.put("vpcId", tg.vpcId());
                    attributes.put("targetType", tg.targetTypeAsString());
                    attributes.put("healthCheckEnabled", tg.healthCheckEnabled());
                    attributes.put("loadBalancerArns", tg.loadBalancerArns());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(tg.targetGroupName())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(tg)
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
            log.error("TargetGroupCollector ELBv2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect target groups", e);
        }
        catch (Exception e)
        {
            log.error("TargetGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting target groups", e);
        }
    }
}
