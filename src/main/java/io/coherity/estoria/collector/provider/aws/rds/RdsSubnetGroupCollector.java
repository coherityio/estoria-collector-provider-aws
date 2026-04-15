package io.coherity.estoria.collector.provider.aws.rds;

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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.awssdk.services.rds.model.RdsException;

/**
 * Collects RDS DB subnet groups via the RDS DescribeDBSubnetGroups API.
 */
@Slf4j
public class RdsSubnetGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsSubnetGroup";

    private RdsClient rdsClient;

    public RdsSubnetGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "networking", "aws")).build());
        log.debug("RdsSubnetGroupCollector created");
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
        log.debug("RdsSubnetGroupCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeDbSubnetGroupsRequest.Builder requestBuilder = DescribeDbSubnetGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsSubnetGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbSubnetGroupsResponse response = this.rdsClient.describeDBSubnetGroups(requestBuilder.build());
            List<DBSubnetGroup> subnetGroups = response.dbSubnetGroups();
            String nextMarker = response.marker();

            log.debug("RdsSubnetGroupCollector received {} subnet groups, nextMarker={}",
                subnetGroups != null ? subnetGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subnetGroups != null)
            {
                for (DBSubnetGroup group : subnetGroups)
                {
                    if (group == null) continue;

                    String groupName = group.dbSubnetGroupName();
                    String arn = group.dbSubnetGroupArn();

                    List<String> subnetIds = group.subnets() == null ? List.of()
                        : group.subnets().stream()
                            .map(s -> s.subnetIdentifier())
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbSubnetGroupName", groupName);
                    attributes.put("dbSubnetGroupArn", arn);
                    attributes.put("dbSubnetGroupDescription", group.dbSubnetGroupDescription());
                    attributes.put("vpcId", group.vpcId());
                    attributes.put("subnetGroupStatus", group.subnetGroupStatus());
                    attributes.put("subnetIds", subnetIds);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : groupName)
                            .qualifiedResourceName(arn != null ? arn : groupName)
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
        catch (RdsException e)
        {
            log.error("RdsSubnetGroupCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS subnet groups", e);
        }
        catch (Exception e)
        {
            log.error("RdsSubnetGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS subnet groups", e);
        }
    }
}
