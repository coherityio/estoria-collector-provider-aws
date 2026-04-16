package io.coherity.estoria.collector.provider.aws.monitoring;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;

@Slf4j
public class CloudWatchLogGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudWatchLogGroup";


    public CloudWatchLogGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "cloudwatch", "logs", "log-group", "aws")).build());
    }

    @Override public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }
    @Override public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }
    @Override public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        CloudWatchLogsClient cloudWatchLogsClient = AwsClientFactory.getInstance().getCloudWatchLogsClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeLogGroupsRequest.Builder requestBuilder = DescribeLogGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeLogGroupsResponse response = cloudWatchLogsClient.describeLogGroups(requestBuilder.build());
            List<LogGroup> logGroups = response.logGroups();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (logGroups != null)
            {
                for (LogGroup logGroup : logGroups)
                {
                    if (logGroup == null)
                    {
                        continue;
                    }

                    String logGroupName = logGroup.logGroupName();
                    String qualifiedName = ARNHelper.cloudWatchLogGroupArn(region, accountId, logGroupName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("logGroupName", logGroupName);
                    attributes.put("arn", logGroup.arn());
                    attributes.put("logGroupArn", logGroup.logGroupArn());
                    attributes.put("creationTime", logGroup.creationTime());
                    attributes.put("retentionInDays", logGroup.retentionInDays());
                    attributes.put("metricFilterCount", logGroup.metricFilterCount());
                    attributes.put("storedBytes", logGroup.storedBytes());
                    attributes.put("kmsKeyId", logGroup.kmsKeyId());
                    attributes.put("dataProtectionStatus", logGroup.dataProtectionStatusAsString());
                    attributes.put("inheritedProperties", logGroup.inheritedPropertiesAsStrings());
                    attributes.put("logGroupClass", logGroup.logGroupClassAsString());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(logGroupName)
                            .qualifiedResourceName(qualifiedName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(logGroupName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(logGroup)
                        .collectedAt(now)
                        .build());
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
                    return Optional.ofNullable(finalNextToken).filter(token -> !token.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (Exception e)
        {
            log.error("CloudWatchLogGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudWatch log groups", e);
        }
    }
}