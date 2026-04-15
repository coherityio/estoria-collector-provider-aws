package io.coherity.estoria.collector.provider.aws.streaming;

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
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;
import software.amazon.awssdk.services.kinesis.model.StreamSummary;

/**
 * Collects Kinesis Data Streams via the Kinesis ListStreams / DescribeStreamSummary APIs.
 */
@Slf4j
public class KinesisStreamCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "KinesisStream";

    private KinesisClient kinesisClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("streaming", "kinesis", "aws"))
            .build();

    public KinesisStreamCollector()
    {
        log.debug("KinesisStreamCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
        log.debug("KinesisStreamCollector.collectEntities called");

        if (this.kinesisClient == null)
        {
            this.kinesisClient = AwsClientFactory.getInstance().getKinesisClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListStreamsRequest.Builder requestBuilder = ListStreamsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("KinesisStreamCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListStreamsResponse response = this.kinesisClient.listStreams(requestBuilder.build());
            List<StreamSummary> streamSummaries = response.streamSummaries();
            String nextToken = response.nextToken();

            log.debug("KinesisStreamCollector received {} streams, nextToken={}",
                streamSummaries != null ? streamSummaries.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (streamSummaries != null)
            {
                for (StreamSummary summary : streamSummaries)
                {
                    if (summary == null) continue;

                    String streamArn  = summary.streamARN();
                    String streamName = summary.streamName();

                    // Fetch detailed info per stream
                    StreamDescriptionSummary detail = null;
                    try
                    {
                        detail = this.kinesisClient
                            .describeStreamSummary(DescribeStreamSummaryRequest.builder()
                                .streamName(streamName)
                                .build())
                            .streamDescriptionSummary();
                    }
                    catch (Exception ex)
                    {
                        log.warn("KinesisStreamCollector could not describe stream {}: {}", streamName, ex.getMessage());
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("streamName",   streamName);
                    attributes.put("streamArn",    streamArn);
                    attributes.put("accountId",    accountId);
                    attributes.put("region",       region);
                    attributes.put("streamStatus", summary.streamStatusAsString());
                    attributes.put("streamModeDetails",
                        summary.streamModeDetails() != null ? summary.streamModeDetails().streamModeAsString() : null);

                    if (detail != null)
                    {
                        attributes.put("shardCount",         detail.openShardCount());
                        attributes.put("retentionPeriodHours", detail.retentionPeriodHours());
                        attributes.put("encryptionType",     detail.encryptionTypeAsString());
                        attributes.put("keyId",              detail.keyId());
                        attributes.put("consumerCount",      detail.consumerCount());
                        attributes.put("streamCreationTimestamp",
                            detail.streamCreationTimestamp() != null ? detail.streamCreationTimestamp().toString() : null);
                        attributes.put("enhancedMonitoring",
                            detail.enhancedMonitoring() != null
                                ? detail.enhancedMonitoring().stream()
                                    .map(e -> e.shardLevelMetricsAsStrings())
                                    .toList()
                                : null);
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(streamArn)
                            .qualifiedResourceName(streamArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(streamName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(summary)
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
        catch (KinesisException e)
        {
            log.error("KinesisStreamCollector Kinesis error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Kinesis streams", e);
        }
        catch (Exception e)
        {
            log.error("KinesisStreamCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Kinesis streams", e);
        }
    }
}
