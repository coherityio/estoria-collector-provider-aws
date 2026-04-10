package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribePlacementGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribePlacementGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.PlacementGroup;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 placement groups via the EC2 DescribePlacementGroups API.
 * Note: DescribePlacementGroups does not support pagination; it returns all results.
 */
@Slf4j
public class Ec2PlacementGroupCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "Ec2PlacementGroup";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "placement-group", "aws"))
            .build();

    public Ec2PlacementGroupCollector()
    {
        log.debug("Ec2PlacementGroupCollector created");
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
        log.debug("Ec2PlacementGroupCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = resolveRegion(providerContext);
            String accountId = resolveAccountId(providerContext);

            // DescribePlacementGroups does not support pagination
            DescribePlacementGroupsResponse response = this.ec2Client.describePlacementGroups(
                DescribePlacementGroupsRequest.builder().build());
            List<PlacementGroup> groups = response.placementGroups();

            log.debug("Ec2PlacementGroupCollector received {} placement groups", 
                groups != null ? groups.size() : 0);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (groups != null)
            {
                for (PlacementGroup group : groups)
                {
                    if (group == null) continue;

                    String groupId   = group.groupId();
                    String groupName = group.groupName();
                    String arn = ARNHelper.ec2PlacementGroupArn(region, accountId, groupId);

                    Map<String, String> tags = group.tags() == null ? Map.of()
                        : group.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("groupId", groupId);
                    attributes.put("groupName", groupName);
                    attributes.put("strategy", group.strategyAsString());
                    attributes.put("state", group.stateAsString());
                    attributes.put("partitionCount", group.partitionCount());
                    attributes.put("spreadLevel", group.spreadLevelAsString());
                    attributes.put("tags", tags);

                    String name = groupName != null ? groupName : groupId;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(groupId)
                            .qualifiedResourceName(arn)
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

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (Ec2Exception e)
        {
            log.error("Ec2PlacementGroupCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 placement groups", e);
        }
        catch (Exception e)
        {
            log.error("Ec2PlacementGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 placement groups", e);
        }
    }

    private static String resolveRegion(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("region");
            if (v != null) return v.toString();
        }
        return null;
    }

    private static String resolveAccountId(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("accountId");
            if (v != null) return v.toString();
        }
        return null;
    }
}
