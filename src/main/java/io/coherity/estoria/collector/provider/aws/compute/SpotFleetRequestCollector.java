package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSpotFleetRequestsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSpotFleetRequestsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.SpotFleetRequestConfig;
import software.amazon.awssdk.services.ec2.model.SpotFleetRequestConfigData;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 Spot Fleet Requests via the EC2 DescribeSpotFleetRequests API.
 */
@Slf4j
public class SpotFleetRequestCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SpotFleetRequest";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "spot", "fleet", "aws"))
            .build();

    public SpotFleetRequestCollector()
    {
        log.debug("SpotFleetRequestCollector created");
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
        log.debug("SpotFleetRequestCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        String region    = resolveRegion(providerContext);
        String accountId = resolveAccountId(providerContext);

        try
        {
            DescribeSpotFleetRequestsRequest.Builder requestBuilder =
                DescribeSpotFleetRequestsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("SpotFleetRequestCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeSpotFleetRequestsResponse response =
                this.ec2Client.describeSpotFleetRequests(requestBuilder.build());
            List<SpotFleetRequestConfig> configs = response.spotFleetRequestConfigs();
            String nextToken = response.nextToken();

            log.debug("SpotFleetRequestCollector received {} spot fleet requests, nextToken={}",
                configs != null ? configs.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (configs != null)
            {
                for (SpotFleetRequestConfig config : configs)
                {
                    if (config == null) continue;

                    String id  = config.spotFleetRequestId();
                    String arn = ARNHelper.ec2SpotFleetArn(region, accountId, id);

                    SpotFleetRequestConfigData configData = config.spotFleetRequestConfig();

                    // Build tag map from the top-level request tags
                    Map<String, String> tags = new HashMap<>();
                    if (config.tags() != null)
                    {
                        for (Tag tag : config.tags())
                        {
                            tags.put(tag.key(), tag.value());
                        }
                    }

                    String name = tags.getOrDefault("Name", id);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("spotFleetRequestId", id);
                    attributes.put("spotFleetRequestArn", arn);
                    attributes.put("spotFleetRequestState", config.spotFleetRequestStateAsString());
                    attributes.put("activityStatus", config.activityStatusAsString());
                    attributes.put("createTime", config.createTime() != null
                        ? config.createTime().toString() : null);

                    if (configData != null)
                    {
                        attributes.put("iamFleetRole", configData.iamFleetRole());
                        attributes.put("targetCapacity", configData.targetCapacity());
                        attributes.put("onDemandTargetCapacity", configData.onDemandTargetCapacity());
                        attributes.put("allocationStrategy", configData.allocationStrategyAsString());
                        attributes.put("onDemandAllocationStrategy",
                            configData.onDemandAllocationStrategyAsString());
                        attributes.put("excessCapacityTerminationPolicy",
                            configData.excessCapacityTerminationPolicyAsString());
                        attributes.put("fulfilledCapacity", configData.fulfilledCapacity());
                        attributes.put("onDemandFulfilledCapacity", configData.onDemandFulfilledCapacity());
                        attributes.put("type", configData.typeAsString());
                        attributes.put("validFrom", configData.validFrom() != null
                            ? configData.validFrom().toString() : null);
                        attributes.put("validUntil", configData.validUntil() != null
                            ? configData.validUntil().toString() : null);
                        attributes.put("terminateInstancesWithExpiration",
                            configData.terminateInstancesWithExpiration());
                        attributes.put("replaceUnhealthyInstances", configData.replaceUnhealthyInstances());
                        attributes.put("instanceInterruptionBehavior",
                            configData.instanceInterruptionBehaviorAsString());
                    }

                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
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
        catch (Ec2Exception e)
        {
            log.error("SpotFleetRequestCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Spot Fleet requests", e);
        }
        catch (Exception e)
        {
            log.error("SpotFleetRequestCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Spot Fleet requests", e);
        }
    }

    private String resolveRegion(ProviderContext providerContext)
    {
        if (providerContext == null || providerContext.getAttributes() == null)
        {
            return "";
        }
        Object val = providerContext.getAttributes().get("region");
        return val != null ? val.toString() : "";
    }

    private String resolveAccountId(ProviderContext providerContext)
    {
        if (providerContext == null || providerContext.getAttributes() == null)
        {
            return "";
        }
        Object val = providerContext.getAttributes().get("accountId");
        return val != null ? val.toString() : "";
    }
}
