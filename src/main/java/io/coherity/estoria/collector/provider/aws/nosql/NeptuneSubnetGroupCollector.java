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
import software.amazon.awssdk.services.neptune.NeptuneClient;
import software.amazon.awssdk.services.neptune.model.DBSubnetGroup;
import software.amazon.awssdk.services.neptune.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.neptune.model.DescribeDbSubnetGroupsResponse;
import software.amazon.awssdk.services.neptune.model.NeptuneException;
import software.amazon.awssdk.services.neptune.model.Tag;

/**
 * Collects Neptune DB subnet groups via the Neptune DescribeDBSubnetGroups API.
 */
@Slf4j
public class NeptuneSubnetGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "NeptuneSubnetGroup";


    public NeptuneSubnetGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "neptune", "networking", "aws")).build());
        log.debug("NeptuneSubnetGroupCollector created");
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
        log.debug("NeptuneSubnetGroupCollector.collectEntities called");

        NeptuneClient neptuneClient = AwsClientFactory.getInstance().getNeptuneClient(providerContext);

        try
        {
            DescribeDbSubnetGroupsRequest.Builder requestBuilder = DescribeDbSubnetGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("NeptuneSubnetGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbSubnetGroupsResponse response = neptuneClient.describeDBSubnetGroups(requestBuilder.build());
            List<DBSubnetGroup> subnetGroups = response.dbSubnetGroups();
            String nextMarker = response.marker();

            log.debug("NeptuneSubnetGroupCollector received {} subnet groups, nextMarker={}",
                subnetGroups != null ? subnetGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subnetGroups != null)
            {
                for (DBSubnetGroup group : subnetGroups)
                {
                    if (group == null) continue;

                    String groupName = group.dbSubnetGroupName();
                    String arn       = group.dbSubnetGroupArn();

                    Map<String, String> tags = new HashMap<>();
                    if (arn != null && !arn.isBlank())
                    {
                        tags = neptuneClient.listTagsForResource(r -> r.resourceName(arn))
                            .tagList()
                            .stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
                    }

                    List<String> subnetIds = group.subnets() == null ? List.of()
                        : group.subnets().stream()
                            .map(s -> s.subnetIdentifier())
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbSubnetGroupName", groupName);
                    attributes.put("dbSubnetGroupArn", arn);
                    attributes.put("description", group.dbSubnetGroupDescription());
                    attributes.put("vpcId", group.vpcId());
                    attributes.put("subnetGroupStatus", group.subnetGroupStatus());
                    attributes.put("subnetIds", subnetIds);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(groupName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(group)
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
        catch (NeptuneException e)
        {
            log.error("NeptuneSubnetGroupCollector Neptune error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Neptune subnet groups", e);
        }
        catch (Exception e)
        {
            log.error("NeptuneSubnetGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Neptune subnet groups", e);
        }
    }
}
