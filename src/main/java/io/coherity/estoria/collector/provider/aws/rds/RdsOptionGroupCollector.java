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
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.awssdk.services.rds.model.RdsException;

/**
 * Collects RDS option groups via the RDS DescribeOptionGroups API.
 */
@Slf4j
public class RdsOptionGroupCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "RdsOptionGroup";

    private RdsClient rdsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "rds", "configuration", "aws"))
            .build();

    public RdsOptionGroupCollector()
    {
        log.debug("RdsOptionGroupCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("RdsOptionGroupCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeOptionGroupsRequest.Builder requestBuilder = DescribeOptionGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsOptionGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeOptionGroupsResponse response = this.rdsClient.describeOptionGroups(requestBuilder.build());
            List<OptionGroup> optionGroups = response.optionGroupsList();
            String nextMarker = response.marker();

            log.debug("RdsOptionGroupCollector received {} option groups, nextMarker={}",
                optionGroups != null ? optionGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (optionGroups != null)
            {
                for (OptionGroup group : optionGroups)
                {
                    if (group == null) continue;

                    String groupName = group.optionGroupName();
                    String arn = group.optionGroupArn();

                    List<String> optionNames = group.options() == null ? List.of()
                        : group.options().stream()
                            .map(o -> o.optionName())
                            .collect(Collectors.toList());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("optionGroupName", groupName);
                    attributes.put("optionGroupArn", arn);
                    attributes.put("optionGroupDescription", group.optionGroupDescription());
                    attributes.put("engineName", group.engineName());
                    attributes.put("majorEngineVersion", group.majorEngineVersion());
                    attributes.put("allowsVpcAndNonVpcInstanceMemberships", group.allowsVpcAndNonVpcInstanceMemberships());
                    attributes.put("vpcId", group.vpcId());
                    attributes.put("options", optionNames);

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
            log.error("RdsOptionGroupCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS option groups", e);
        }
        catch (Exception e)
        {
            log.error("RdsOptionGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS option groups", e);
        }
    }
}
