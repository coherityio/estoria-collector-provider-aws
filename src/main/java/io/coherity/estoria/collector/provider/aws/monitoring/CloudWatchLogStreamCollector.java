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
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogStreamsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogStream;
import software.amazon.awssdk.services.cloudwatchlogs.model.OrderBy;

@Slf4j
public class CloudWatchLogStreamCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudWatchLogStream";

    private static final int DEFAULT_STREAMS_PER_GROUP = 25;


    public CloudWatchLogStreamCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "cloudwatch", "logs", "log-stream", "aws")).build());
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

            DescribeLogGroupsRequest.Builder groupRequestBuilder = DescribeLogGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                groupRequestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(groupRequestBuilder::nextToken);

            DescribeLogGroupsResponse groupsResponse = cloudWatchLogsClient.describeLogGroups(groupRequestBuilder.build());
            List<LogGroup> logGroups = groupsResponse.logGroups();
            String nextToken = groupsResponse.nextToken();

            int streamLimit = pageSize > 0 ? Math.min(pageSize, 50) : DEFAULT_STREAMS_PER_GROUP;
            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (logGroups != null)
            {
                for (LogGroup logGroup : logGroups)
                {
                    if (logGroup == null || logGroup.logGroupName() == null)
                    {
                        continue;
                    }

                    DescribeLogStreamsResponse streamsResponse = cloudWatchLogsClient.describeLogStreams(
                        DescribeLogStreamsRequest.builder()
                            .logGroupName(logGroup.logGroupName())
                            .orderBy(OrderBy.LAST_EVENT_TIME)
                            .descending(true)
                            .limit(streamLimit)
                            .build());

                    for (LogStream logStream : streamsResponse.logStreams())
                    {
                        if (logStream == null)
                        {
                            continue;
                        }

                        String streamName = logStream.logStreamName();
                        String streamId = logGroup.logGroupName() + ":" + streamName;
                        String qualifiedName = ARNHelper.cloudWatchLogStreamArn(region, accountId, logGroup.logGroupName(), streamName);

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("logGroupName", logGroup.logGroupName());
                        attributes.put("logStreamName", streamName);
                        attributes.put("arn", logStream.arn());
                        attributes.put("creationTime", logStream.creationTime());
                        attributes.put("firstEventTimestamp", logStream.firstEventTimestamp());
                        attributes.put("lastEventTimestamp", logStream.lastEventTimestamp());
                        attributes.put("lastIngestionTime", logStream.lastIngestionTime());
                        attributes.put("storedBytes", logStream.storedBytes());
                        attributes.put("uploadSequenceToken", logStream.uploadSequenceToken());
                        attributes.put("arnHelperQualifiedName", qualifiedName);
                        attributes.put("accountId", accountId);
                        attributes.put("region", region);

                        entities.add(CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(streamId)
                                .qualifiedResourceName(qualifiedName)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(streamName)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(logStream)
                            .collectedAt(now)
                            .build());
                    }
                }
            }

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());
            metadataValues.put("summaryLevel", true);
            metadataValues.put("streamsPerGroup", streamLimit);

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
            log.error("CloudWatchLogStreamCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudWatch log streams", e);
        }
    }
}