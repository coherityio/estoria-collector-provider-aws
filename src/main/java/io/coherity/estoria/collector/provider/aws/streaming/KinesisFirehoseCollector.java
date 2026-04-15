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
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamDescription;
import software.amazon.awssdk.services.firehose.model.DescribeDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsRequest;
import software.amazon.awssdk.services.firehose.model.ListDeliveryStreamsResponse;

/**
 * Collects Kinesis Data Firehose delivery streams via the Firehose
 * ListDeliveryStreams / DescribeDeliveryStream APIs.
 */
@Slf4j
public class KinesisFirehoseCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "KinesisFirehose";

    private FirehoseClient firehoseClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("streaming", "firehose", "kinesis", "aws"))
            .build();

    public KinesisFirehoseCollector()
    {
        log.debug("KinesisFirehoseCollector created");
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
        log.debug("KinesisFirehoseCollector.collectEntities called");

        if (this.firehoseClient == null)
        {
            this.firehoseClient = AwsClientFactory.getInstance().getFirehoseClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListDeliveryStreamsRequest.Builder requestBuilder = ListDeliveryStreamsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("KinesisFirehoseCollector resuming from exclusiveStartDeliveryStreamName: {}", token);
                requestBuilder.exclusiveStartDeliveryStreamName(token);
            });

            ListDeliveryStreamsResponse response = this.firehoseClient.listDeliveryStreams(requestBuilder.build());
            List<String> streamNames  = response.deliveryStreamNames();
            Boolean      hasMore      = response.hasMoreDeliveryStreams();
            String       lastStream   = (streamNames != null && !streamNames.isEmpty())
                                        ? streamNames.get(streamNames.size() - 1) : null;

            log.debug("KinesisFirehoseCollector received {} delivery streams, hasMore={}",
                streamNames != null ? streamNames.size() : 0, hasMore);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (streamNames != null)
            {
                for (String streamName : streamNames)
                {
                    if (streamName == null || streamName.isBlank()) continue;

                    DeliveryStreamDescription detail = null;
                    try
                    {
                        detail = this.firehoseClient
                            .describeDeliveryStream(DescribeDeliveryStreamRequest.builder()
                                .deliveryStreamName(streamName)
                                .build())
                            .deliveryStreamDescription();
                    }
                    catch (Exception ex)
                    {
                        log.warn("KinesisFirehoseCollector could not describe delivery stream {}: {}",
                            streamName, ex.getMessage());
                    }

                    String streamArn = detail != null ? detail.deliveryStreamARN() : null;
                    String entityId  = streamArn != null ? streamArn
                        : "arn:aws:firehose:" + region + ":" + accountId + ":deliverystream/" + streamName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("deliveryStreamName", streamName);
                    attributes.put("deliveryStreamArn",  streamArn);
                    attributes.put("accountId",          accountId);
                    attributes.put("region",             region);

                    if (detail != null)
                    {
                        attributes.put("deliveryStreamStatus", detail.deliveryStreamStatusAsString());
                        attributes.put("deliveryStreamType",   detail.deliveryStreamTypeAsString());
                        attributes.put("versionId",            detail.versionId());
                        attributes.put("createTimestamp",
                            detail.createTimestamp() != null ? detail.createTimestamp().toString() : null);
                        attributes.put("lastUpdateTimestamp",
                            detail.lastUpdateTimestamp() != null ? detail.lastUpdateTimestamp().toString() : null);
                        attributes.put("encryptionConfiguration",
                            detail.deliveryStreamEncryptionConfiguration() != null
                                ? detail.deliveryStreamEncryptionConfiguration().statusAsString() : null);
                        attributes.put("sourceType",
                            detail.source() != null && detail.source().kinesisStreamSourceDescription() != null
                                ? "KinesisStream" : "DirectPut");
                        attributes.put("destinationCount",
                            detail.destinations() != null ? detail.destinations().size() : 0);
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(entityId)
                            .qualifiedResourceName(entityId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(streamName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(detail)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            // Firehose pagination uses the last stream name as the cursor
            final String nextCursorToken = Boolean.TRUE.equals(hasMore) ? lastStream : null;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(nextCursorToken);
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (FirehoseException e)
        {
            log.error("KinesisFirehoseCollector Firehose error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Kinesis Firehose delivery streams", e);
        }
        catch (Exception e)
        {
            log.error("KinesisFirehoseCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Kinesis Firehose delivery streams", e);
        }
    }
}
