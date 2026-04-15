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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.ListTopicsRequest;
import software.amazon.awssdk.services.sns.model.ListTopicsResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sns.model.Topic;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;

/**
 * Collects SNS topics via the SNS ListTopics / GetTopicAttributes APIs.
 */
@Slf4j
public class SnsTopicCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SnsTopic";

    private SnsClient snsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("streaming", "messaging", "sns", "topic", "aws"))
            .build();

    public SnsTopicCollector()
    {
        log.debug("SnsTopicCollector created");
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
        log.debug("SnsTopicCollector.collectEntities called");

        if (this.snsClient == null)
        {
            this.snsClient = AwsClientFactory.getInstance().getSnsClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListTopicsRequest.Builder requestBuilder = ListTopicsRequest.builder();

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("SnsTopicCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListTopicsResponse response = this.snsClient.listTopics(requestBuilder.build());
            List<Topic> topics    = response.topics();
            String      nextToken = response.nextToken();

            log.debug("SnsTopicCollector received {} topics, nextToken={}",
                topics != null ? topics.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (topics != null)
            {
                for (Topic topic : topics)
                {
                    if (topic == null) continue;

                    String topicArn  = topic.topicArn();
                    String topicName = topicArn != null ? topicArn.substring(topicArn.lastIndexOf(':') + 1) : null;

                    Map<String, String> topicAttrs = new HashMap<>();
                    try
                    {
                        topicAttrs = this.snsClient
                            .getTopicAttributes(GetTopicAttributesRequest.builder()
                                .topicArn(topicArn)
                                .build())
                            .attributes();
                    }
                    catch (Exception ex)
                    {
                        log.warn("SnsTopicCollector could not get attributes for topic {}: {}",
                            topicArn, ex.getMessage());
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("topicArn",          topicArn);
                    attributes.put("topicName",         topicName);
                    attributes.put("accountId",         accountId);
                    attributes.put("region",            region);
                    attributes.put("displayName",       topicAttrs.get("DisplayName"));
                    attributes.put("owner",             topicAttrs.get("Owner"));
                    attributes.put("subscriptionsConfirmed", topicAttrs.get("SubscriptionsConfirmed"));
                    attributes.put("subscriptionsPending",   topicAttrs.get("SubscriptionsPending"));
                    attributes.put("subscriptionsDeleted",   topicAttrs.get("SubscriptionsDeleted"));
                    attributes.put("policy",            topicAttrs.get("Policy"));
                    attributes.put("deliveryPolicy",    topicAttrs.get("DeliveryPolicy"));
                    attributes.put("effectiveDeliveryPolicy", topicAttrs.get("EffectiveDeliveryPolicy"));
                    attributes.put("kmsMasterKeyId",    topicAttrs.get("KmsMasterKeyId"));
                    attributes.put("fifoTopic",         topicAttrs.get("FifoTopic"));
                    attributes.put("contentBasedDeduplication", topicAttrs.get("ContentBasedDeduplication"));
                    attributes.put("tracingConfig",     topicAttrs.get("TracingConfig"));
                    attributes.put("signatureVersion",  topicAttrs.get("SignatureVersion"));

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(topicArn)
                            .qualifiedResourceName(topicArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(topicName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(topic)
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
        catch (SnsException e)
        {
            log.error("SnsTopicCollector SNS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SNS topics", e);
        }
        catch (Exception e)
        {
            log.error("SnsTopicCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SNS topics", e);
        }
    }
}
