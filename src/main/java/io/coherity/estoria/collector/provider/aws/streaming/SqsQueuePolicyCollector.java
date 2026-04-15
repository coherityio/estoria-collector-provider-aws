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
 * Collects SQS queue policies and dead-letter queue (DLQ) configuration
 * via the SQS ListQueues / GetQueueAttributes APIs.
 */
@Slf4j
public class SqsQueuePolicyCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SqsQueuePolicy";

    private SqsClient sqsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("streaming", "messaging", "sqs", "policy", "aws"))
            .build();

    public SqsQueuePolicyCollector()
    {
        log.debug("SqsQueuePolicyCollector created");
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
        log.debug("SqsQueuePolicyCollector.collectEntities called");

        if (this.sqsClient == null)
        {
            this.sqsClient = AwsClientFactory.getInstance().getSqsClient(providerContext);
        }

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
                log.debug("SqsQueuePolicyCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListQueuesResponse response = this.sqsClient.listQueues(requestBuilder.build());
            List<String> queueUrls = response.queueUrls();
            String       nextToken = response.nextToken();

            log.debug("SqsQueuePolicyCollector processing {} queues for policies, nextToken={}",
                queueUrls != null ? queueUrls.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (queueUrls != null)
            {
                List<QueueAttributeName> attrNames = List.of(
                    QueueAttributeName.QUEUE_ARN,
                    QueueAttributeName.POLICY,
                    QueueAttributeName.REDRIVE_POLICY,
                    QueueAttributeName.REDRIVE_ALLOW_POLICY
                );

                for (String queueUrl : queueUrls)
                {
                    if (queueUrl == null || queueUrl.isBlank()) continue;

                    String queueName = queueUrl.substring(queueUrl.lastIndexOf('/') + 1);

                    Map<String, String> attrs = new HashMap<>();
                    try
                    {
                        attrs = this.sqsClient.getQueueAttributes(
                            GetQueueAttributesRequest.builder()
                                .queueUrl(queueUrl)
                                .attributeNames(attrNames)
                                .build()
                        ).attributesAsStrings();
                    }
                    catch (Exception ex)
                    {
                        log.warn("SqsQueuePolicyCollector could not get attributes for queue {}: {}",
                            queueName, ex.getMessage());
                        continue;
                    }

                    String queueArn   = attrs.getOrDefault(QueueAttributeName.QUEUE_ARN.toString(),
                        "arn:aws:sqs:" + region + ":" + accountId + ":" + queueName);
                    String policyJson = attrs.get(QueueAttributeName.POLICY.toString());

                    // Only emit an entity if there is a policy or redrive config
                    String redrivePolicy      = attrs.get(QueueAttributeName.REDRIVE_POLICY.toString());
                    String redriveAllowPolicy = attrs.get(QueueAttributeName.REDRIVE_ALLOW_POLICY.toString());

                    if (policyJson == null && redrivePolicy == null && redriveAllowPolicy == null)
                    {
                        continue;
                    }

                    String policyId = queueArn + ":policy";

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("queueName",          queueName);
                    attributes.put("queueUrl",           queueUrl);
                    attributes.put("queueArn",           queueArn);
                    attributes.put("accountId",          accountId);
                    attributes.put("region",             region);
                    attributes.put("policyDocument",     policyJson);
                    attributes.put("redrivePolicy",      redrivePolicy);
                    attributes.put("redriveAllowPolicy", redriveAllowPolicy);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(policyId)
                            .qualifiedResourceName(policyId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(queueName + ":policy")
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
            log.error("SqsQueuePolicyCollector SQS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SQS queue policies", e);
        }
        catch (Exception e)
        {
            log.error("SqsQueuePolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SQS queue policies", e);
        }
    }
}
