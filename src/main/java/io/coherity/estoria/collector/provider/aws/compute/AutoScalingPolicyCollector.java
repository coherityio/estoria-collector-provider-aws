package io.coherity.estoria.collector.provider.aws.compute;

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
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingException;
import software.amazon.awssdk.services.autoscaling.model.DescribePoliciesRequest;
import software.amazon.awssdk.services.autoscaling.model.DescribePoliciesResponse;
import software.amazon.awssdk.services.autoscaling.model.ScalingPolicy;

/**
 * Collects Auto Scaling scaling policies via the AutoScaling DescribePolicies API.
 */
@Slf4j
public class AutoScalingPolicyCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "AutoScalingPolicy";

    private AutoScalingClient autoScalingClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "autoscaling", "policy", "aws"))
            .build();

    public AutoScalingPolicyCollector()
    {
        log.debug("AutoScalingPolicyCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("AutoScalingPolicyCollector.collect called");

        if (this.autoScalingClient == null)
        {
            this.autoScalingClient = AwsClientFactory.getInstance().getAutoScalingClient(providerContext);
        }

        try
        {
            DescribePoliciesRequest.Builder requestBuilder = DescribePoliciesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("AutoScalingPolicyCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribePoliciesResponse response = this.autoScalingClient.describePolicies(requestBuilder.build());
            List<ScalingPolicy> policies = response.scalingPolicies();
            String nextToken = response.nextToken();

            log.debug("AutoScalingPolicyCollector received {} scaling policies, nextToken={}",
                policies != null ? policies.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (policies != null)
            {
                for (ScalingPolicy policy : policies)
                {
                    if (policy == null) continue;

                    // Policies have their own ARN
                    String arn        = policy.policyARN();
                    String policyName = policy.policyName();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("policyName", policyName);
                    attributes.put("policyArn", arn);
                    attributes.put("policyType", policy.policyType());
                    attributes.put("autoScalingGroupName", policy.autoScalingGroupName());
                    attributes.put("adjustmentType", policy.adjustmentType());
                    attributes.put("scalingAdjustment", policy.scalingAdjustment());
                    attributes.put("cooldown", policy.cooldown());
                    attributes.put("estimatedInstanceWarmup", policy.estimatedInstanceWarmup());
                    attributes.put("enabled", policy.enabled());
                    attributes.put("metricAggregationType", policy.metricAggregationType());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : policyName)
                            .qualifiedResourceName(arn != null ? arn : policyName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(policyName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(policy)
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
        catch (AutoScalingException e)
        {
            log.error("AutoScalingPolicyCollector AutoScaling error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Auto Scaling policies", e);
        }
        catch (Exception e)
        {
            log.error("AutoScalingPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Auto Scaling policies", e);
        }
    }
}
