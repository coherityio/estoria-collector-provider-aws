package io.coherity.estoria.collector.provider.aws.loadbalance;

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
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2Client;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetHealthResponse;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.ElasticLoadBalancingV2Exception;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.TargetGroup;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.TargetHealthDescription;

/**
 * Collects target health state by iterating all target groups and calling
 * DescribeTargetHealth per group. Each result represents one registered target's health.
 */
@Slf4j
public class TargetHealthCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "TargetHealth";


    public TargetHealthCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("loadbalance", "target-health", "aws")).build());
        log.debug("TargetHealthCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.ACCOUNT_REGIONAL;
    }

    @Override
    public EntityCategory getEntityCategory()
    {
        return EntityCategory.RESOURCE;
    }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("TargetHealthCollector.collect called");

        ElasticLoadBalancingV2Client elbV2Client = AwsClientFactory.getInstance().getElbV2Client(providerContext);

        try
        {
            // Collect all target group ARNs first (no pagination token support at this level)
            List<String> targetGroupArns = new ArrayList<>();
            String tgMarker = null;
            do
            {
                DescribeTargetGroupsRequest.Builder tgRequestBuilder = DescribeTargetGroupsRequest.builder();
                if (tgMarker != null) tgRequestBuilder.marker(tgMarker);

                var tgResponse = elbV2Client.describeTargetGroups(tgRequestBuilder.build());
                for (TargetGroup tg : tgResponse.targetGroups())
                {
                    targetGroupArns.add(tg.targetGroupArn());
                }
                tgMarker = tgResponse.nextMarker();
            }
            while (tgMarker != null && !tgMarker.isBlank());

            log.debug("TargetHealthCollector found {} target groups to query", targetGroupArns.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String tgArn : targetGroupArns)
            {
                try
                {
                    DescribeTargetHealthResponse healthResponse = elbV2Client.describeTargetHealth(
                        DescribeTargetHealthRequest.builder().targetGroupArn(tgArn).build());

                    for (TargetHealthDescription thd : healthResponse.targetHealthDescriptions())
                    {
                        if (thd == null || thd.target() == null) continue;

                        String targetId = thd.target().id();
                        // Synthetic ID: tgArn + "/" + targetId
                        String syntheticId = tgArn + "/" + targetId;

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("targetGroupArn", tgArn);
                        attributes.put("targetId", targetId);
                        attributes.put("targetPort", thd.target().port());
                        attributes.put("healthState",
                            thd.targetHealth() != null ? thd.targetHealth().stateAsString() : null);
                        attributes.put("healthReason",
                            thd.targetHealth() != null ? thd.targetHealth().reasonAsString() : null);
                        attributes.put("healthDescription",
                            thd.targetHealth() != null ? thd.targetHealth().description() : null);
                        attributes.put("anomalyDetection",
                            thd.anomalyDetection() != null ? thd.anomalyDetection().resultAsString() : null);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(syntheticId)
                                .qualifiedResourceName(syntheticId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(targetId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(thd)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                }
                catch (ElasticLoadBalancingV2Exception e)
                {
                    log.warn("TargetHealthCollector could not describe health for TG {}: {}", tgArn,
                        e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage());
                }
            }

            log.debug("TargetHealthCollector collected {} target health entries", entities.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            // TargetHealth has no pagination token — always returns empty next token
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
        catch (ElasticLoadBalancingV2Exception e)
        {
            log.error("TargetHealthCollector ELBv2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect target health", e);
        }
        catch (Exception e)
        {
            log.error("TargetHealthCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting target health", e);
        }
    }
}
