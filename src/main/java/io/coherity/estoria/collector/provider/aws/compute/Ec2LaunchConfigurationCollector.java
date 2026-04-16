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
import software.amazon.awssdk.services.autoscaling.model.DescribeLaunchConfigurationsRequest;
import software.amazon.awssdk.services.autoscaling.model.DescribeLaunchConfigurationsResponse;
import software.amazon.awssdk.services.autoscaling.model.LaunchConfiguration;

/**
 * Collects EC2 Auto Scaling launch configurations (legacy) via the AutoScaling
 * DescribeLaunchConfigurations API.
 */
@Slf4j
public class Ec2LaunchConfigurationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "Ec2LaunchConfiguration";


    public Ec2LaunchConfigurationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("compute", "autoscaling", "launch-configuration", "aws")).build());
        log.debug("Ec2LaunchConfigurationCollector created");
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
        log.debug("Ec2LaunchConfigurationCollector.collect called");

        AutoScalingClient autoScalingClient = AwsClientFactory.getInstance().getAutoScalingClient(providerContext);

        try
        {
            DescribeLaunchConfigurationsRequest.Builder requestBuilder =
                DescribeLaunchConfigurationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2LaunchConfigurationCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeLaunchConfigurationsResponse response =
                autoScalingClient.describeLaunchConfigurations(requestBuilder.build());
            List<LaunchConfiguration> configs = response.launchConfigurations();
            String nextToken = response.nextToken();

            log.debug("Ec2LaunchConfigurationCollector received {} launch configurations, nextToken={}",
                configs != null ? configs.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (configs != null)
            {
                for (LaunchConfiguration config : configs)
                {
                    if (config == null) continue;

                    // Launch configurations have their own ARN in the API response
                    String arn  = config.launchConfigurationARN();
                    String name = config.launchConfigurationName();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("launchConfigurationName", name);
                    attributes.put("launchConfigurationArn", arn);
                    attributes.put("imageId", config.imageId());
                    attributes.put("instanceType", config.instanceType());
                    attributes.put("keyName", config.keyName());
                    attributes.put("iamInstanceProfile", config.iamInstanceProfile());
                    attributes.put("userData", "<redacted>");
                    attributes.put("ebsOptimized", config.ebsOptimized());
                    attributes.put("associatePublicIpAddress", config.associatePublicIpAddress());
                    attributes.put("spotPrice", config.spotPrice());
                    attributes.put("createdTime",
                        config.createdTime() != null ? config.createdTime().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(name)
                            .qualifiedResourceName(arn != null ? arn : name)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(config)
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
            log.error("Ec2LaunchConfigurationCollector AutoScaling error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 launch configurations", e);
        }
        catch (Exception e)
        {
            log.error("Ec2LaunchConfigurationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 launch configurations", e);
        }
    }
}
