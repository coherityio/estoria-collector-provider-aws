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
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * Collects SQS queues (standard and FIFO) via the SQS ListQueues / GetQueueAttributes APIs.
 */
@Slf4j
public class SqsQueueCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SqsQueue";


    public SqsQueueCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "messaging", "sqs", "queue", "aws")).build());
        log.debug("SqsQueueCollector created");
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
        log.debug("SqsQueueCollector.collectEntities called");

        SqsClient sqsClient = AwsClientFactory.getInstance().getSqsClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListQueuesRequest.Builder requestBuilder = ListQueuesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("SqsQueueCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListQueuesResponse response = sqsClient.listQueues(requestBuilder.build());
            List<String> queueUrls  = response.queueUrls();
            String       nextToken  = response.nextToken();

            log.debug("SqsQueueCollector received {} queues, nextToken={}",
                queueUrls != null ? queueUrls.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (queueUrls != null)
            {
                List<QueueAttributeName> attrNames = List.of(
                    QueueAttributeName.QUEUE_ARN,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED,
                    QueueAttributeName.CREATED_TIMESTAMP,
                    QueueAttributeName.LAST_MODIFIED_TIMESTAMP,
                    QueueAttributeName.VISIBILITY_TIMEOUT,
                    QueueAttributeName.MAXIMUM_MESSAGE_SIZE,
                    QueueAttributeName.MESSAGE_RETENTION_PERIOD,
                    QueueAttributeName.DELAY_SECONDS,
                    QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS,
                    QueueAttributeName.FIFO_QUEUE,
                    QueueAttributeName.CONTENT_BASED_DEDUPLICATION,
                    QueueAttributeName.KMS_MASTER_KEY_ID,
                    QueueAttributeName.REDRIVE_POLICY
                );

                for (String queueUrl : queueUrls)
                {
                    if (queueUrl == null || queueUrl.isBlank()) continue;

                    // Queue name is the last segment of the URL
                    String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);

                    Map<String, String> attrs = new HashMap<>();
                    try
                    {
                        attrs = sqsClient.getQueueAttributes(
                            GetQueueAttributesRequest.builder()
                                .queueUrl(queueUrl)
                                .attributeNames(attrNames)
                                .build()
                        ).attributesAsStrings();
                    }
                    catch (Exception ex)
                    {
                        log.warn("SqsQueueCollector could not get attributes for queue {}: {}",
                            queueName, ex.getMessage());
                    }

                    String queueArn = attrs.getOrDefault(QueueAttributeName.QUEUE_ARN.toString(),
                        "arn:aws:sqs:" + region + ":" + accountId + ":" + queueName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("queueName",            queueName);
                    attributes.put("queueUrl",             queueUrl);
                    attributes.put("queueArn",             queueArn);
                    attributes.put("accountId",            accountId);
                    attributes.put("region",               region);
                    attributes.put("approximateNumberOfMessages",
                        attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES.toString()));
                    attributes.put("approximateNumberOfMessagesNotVisible",
                        attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE.toString()));
                    attributes.put("approximateNumberOfMessagesDelayed",
                        attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED.toString()));
                    attributes.put("createdTimestamp",
                        attrs.get(QueueAttributeName.CREATED_TIMESTAMP.toString()));
                    attributes.put("lastModifiedTimestamp",
                        attrs.get(QueueAttributeName.LAST_MODIFIED_TIMESTAMP.toString()));
                    attributes.put("visibilityTimeoutSeconds",
                        attrs.get(QueueAttributeName.VISIBILITY_TIMEOUT.toString()));
                    attributes.put("maximumMessageSizeBytes",
                        attrs.get(QueueAttributeName.MAXIMUM_MESSAGE_SIZE.toString()));
                    attributes.put("messageRetentionPeriodSeconds",
                        attrs.get(QueueAttributeName.MESSAGE_RETENTION_PERIOD.toString()));
                    attributes.put("delaySeconds",
                        attrs.get(QueueAttributeName.DELAY_SECONDS.toString()));
                    attributes.put("receiveMessageWaitTimeSeconds",
                        attrs.get(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS.toString()));
                    attributes.put("isFifo",
                        "true".equalsIgnoreCase(attrs.get(QueueAttributeName.FIFO_QUEUE.toString())));
                    attributes.put("contentBasedDeduplication",
                        attrs.get(QueueAttributeName.CONTENT_BASED_DEDUPLICATION.toString()));
                    attributes.put("kmsMasterKeyId",
                        attrs.get(QueueAttributeName.KMS_MASTER_KEY_ID.toString()));
                    attributes.put("redrivePolicy",
                        attrs.get(QueueAttributeName.REDRIVE_POLICY.toString()));

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(queueArn)
                            .qualifiedResourceName(queueArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(queueName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(attrs)
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
        catch (SqsException e)
        {
            log.error("SqsQueueCollector SQS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SQS queues", e);
        }
        catch (Exception e)
        {
            log.error("SqsQueueCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SQS queues", e);
        }
    }
}
