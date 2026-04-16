package io.coherity.estoria.collector.provider.aws.nosql;

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
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.model.ClusterParameterGroup;
import software.amazon.awssdk.services.redshift.model.DescribeClusterParameterGroupsRequest;
import software.amazon.awssdk.services.redshift.model.DescribeClusterParameterGroupsResponse;
import software.amazon.awssdk.services.redshift.model.RedshiftException;

/**
 * Collects Redshift cluster parameter groups via the Redshift DescribeClusterParameterGroups API.
 */
@Slf4j
public class RedshiftParameterGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RedshiftParameterGroup";


    public RedshiftParameterGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "redshift", "configuration", "aws")).build());
        log.debug("RedshiftParameterGroupCollector created");
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
        log.debug("RedshiftParameterGroupCollector.collectEntities called");

        RedshiftClient redshiftClient = AwsClientFactory.getInstance().getRedshiftClient(providerContext);

        try
        {
            DescribeClusterParameterGroupsRequest.Builder requestBuilder =
                DescribeClusterParameterGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RedshiftParameterGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeClusterParameterGroupsResponse response =
                redshiftClient.describeClusterParameterGroups(requestBuilder.build());
            List<ClusterParameterGroup> paramGroups = response.parameterGroups();
            String nextMarker = response.marker();

            log.debug("RedshiftParameterGroupCollector received {} parameter groups, nextMarker={}",
                paramGroups != null ? paramGroups.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (paramGroups != null)
            {
                for (ClusterParameterGroup group : paramGroups)
                {
                    if (group == null) continue;

                    String groupName = group.parameterGroupName();
                    String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : "";
                    String accountId = awsSessionContext.getCurrentAccountId();
                    String arn = "arn:aws:redshift:" + region + ":" + accountId
                        + ":parametergroup:" + groupName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("parameterGroupName", groupName);
                    attributes.put("parameterGroupFamily", group.parameterGroupFamily());
                    attributes.put("description", group.description());

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
        catch (RedshiftException e)
        {
            log.error("RedshiftParameterGroupCollector Redshift error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Redshift parameter groups", e);
        }
        catch (Exception e)
        {
            log.error("RedshiftParameterGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Redshift parameter groups", e);
        }
    }
}
