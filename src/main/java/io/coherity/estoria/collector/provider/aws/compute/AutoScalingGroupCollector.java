package io.coherity.estoria.collector.provider.aws.compute;

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
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingException;
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsResponse;
import software.amazon.awssdk.services.autoscaling.model.TagDescription;

/**
 * Collects Auto Scaling groups via the AutoScaling DescribeAutoScalingGroups API.
 */
@Slf4j
public class AutoScalingGroupCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "AutoScalingGroup";

    private AutoScalingClient autoScalingClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "autoscaling", "asg", "aws"))
            .build();

    public AutoScalingGroupCollector()
    {
        log.debug("AutoScalingGroupCollector created");
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
        log.debug("AutoScalingGroupCollector.collect called");

        if (this.autoScalingClient == null)
        {
            this.autoScalingClient = AwsClientFactory.getInstance().getAutoScalingClient(providerContext);
        }

        try
        {
            DescribeAutoScalingGroupsRequest.Builder requestBuilder =
                DescribeAutoScalingGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("AutoScalingGroupCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeAutoScalingGroupsResponse response =
                this.autoScalingClient.describeAutoScalingGroups(requestBuilder.build());
            List<AutoScalingGroup> groups = response.autoScalingGroups();
            String nextToken = response.nextToken();

            log.debug("AutoScalingGroupCollector received {} ASGs, nextToken={}",
                groups != null ? groups.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (groups != null)
            {
                for (AutoScalingGroup group : groups)
                {
                    if (group == null) continue;

                    // ASGs have their own ARN in the API response
                    String arn  = group.autoScalingGroupARN();
                    String name = group.autoScalingGroupName();

                    Map<String, String> tags = group.tags() == null ? Map.of()
                        : group.tags().stream()
                            .collect(Collectors.toMap(TagDescription::key, TagDescription::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("autoScalingGroupName", name);
                    attributes.put("autoScalingGroupArn", arn);
                    attributes.put("minSize", group.minSize());
                    attributes.put("maxSize", group.maxSize());
                    attributes.put("desiredCapacity", group.desiredCapacity());
                    attributes.put("status", group.status());
                    attributes.put("healthCheckType", group.healthCheckType());
                    attributes.put("vpcZoneIdentifier", group.vpcZoneIdentifier());
                    attributes.put("availabilityZones", group.availabilityZones());
                    attributes.put("launchConfigurationName", group.launchConfigurationName());
                    attributes.put("launchTemplateId",
                        group.launchTemplate() != null ? group.launchTemplate().launchTemplateId() : null);
                    attributes.put("launchTemplateVersion",
                        group.launchTemplate() != null ? group.launchTemplate().version() : null);
                    attributes.put("createdTime",
                        group.createdTime() != null ? group.createdTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(name)
                            .qualifiedResourceName(arn != null ? arn : name)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(group)
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
            log.error("AutoScalingGroupCollector AutoScaling error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Auto Scaling groups", e);
        }
        catch (Exception e)
        {
            log.error("AutoScalingGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Auto Scaling groups", e);
        }
    }
}
