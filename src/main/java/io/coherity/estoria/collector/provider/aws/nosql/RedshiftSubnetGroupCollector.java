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
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.model.ClusterSubnetGroup;
import software.amazon.awssdk.services.redshift.model.DescribeClusterSubnetGroupsRequest;
import software.amazon.awssdk.services.redshift.model.DescribeClusterSubnetGroupsResponse;
import software.amazon.awssdk.services.redshift.model.RedshiftException;
import software.amazon.awssdk.services.redshift.model.Tag;

/**
 * Collects Redshift cluster subnet groups via the Redshift DescribeClusterSubnetGroups API.
 */
@Slf4j
public class RedshiftSubnetGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RedshiftSubnetGroup";


    public RedshiftSubnetGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "redshift", "networking", "aws")).build());
        log.debug("RedshiftSubnetGroupCollector created");
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
        log.debug("RedshiftSubnetGroupCollector.collectEntities called");

        RedshiftClient redshiftClient = AwsClientFactory.getInstance().getRedshiftClient(providerContext);

        try
        {
            DescribeClusterSubnetGroupsRequest.Builder requestBuilder =
                DescribeClusterSubnetGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RedshiftSubnetGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeClusterSubnetGroupsResponse response =
                redshiftClient.describeClusterSubnetGroups(requestBuilder.build());
            List<ClusterSubnetGroup> subnetGroups = response.clusterSubnetGroups();
            String nextMarker = response.marker();

            log.debug("RedshiftSubnetGroupCollector received {} subnet groups, nextMarker={}",
                subnetGroups != null ? subnetGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subnetGroups != null)
            {
                for (ClusterSubnetGroup group : subnetGroups)
                {
                    if (group == null) continue;

                    String groupName = group.clusterSubnetGroupName();
                    String arn = group.subnetGroupStatus(); // not an ARN; build one

                    String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : "";
                    String accountId = awsSessionContext.getCurrentAccountId();
                    String builtArn  = "arn:aws:redshift:" + region + ":" + accountId
                        + ":subnetgroup:" + groupName;

                    List<String> subnetIds = group.subnets() == null ? List.of()
                        : group.subnets().stream()
                            .map(s -> s.subnetIdentifier())
                            .collect(Collectors.toList());

                    Map<String, String> tags = group.tags() == null ? new HashMap<>()
                        : group.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("clusterSubnetGroupName", groupName);
                    attributes.put("description", group.description());
                    attributes.put("vpcId", group.vpcId());
                    attributes.put("subnetGroupStatus", group.subnetGroupStatus());
                    attributes.put("subnetIds", subnetIds);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(builtArn)
                            .qualifiedResourceName(builtArn)
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
        catch (RedshiftException e)
        {
            log.error("RedshiftSubnetGroupCollector Redshift error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Redshift subnet groups", e);
        }
        catch (Exception e)
        {
            log.error("RedshiftSubnetGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Redshift subnet groups", e);
        }
    }
}
