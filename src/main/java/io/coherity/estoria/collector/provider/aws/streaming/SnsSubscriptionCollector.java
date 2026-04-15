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
import software.amazon.awssdk.services.sns.model.ListSubscriptionsRequest;
import software.amazon.awssdk.services.sns.model.ListSubscriptionsResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sns.model.Subscription;

/**
 * Collects SNS subscriptions via the SNS ListSubscriptions API.
 */
@Slf4j
public class SnsSubscriptionCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SnsSubscription";

    private SnsClient snsClient;

    public SnsSubscriptionCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "messaging", "sns", "subscription", "aws")).build());
        log.debug("SnsSubscriptionCollector created");
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
        log.debug("SnsSubscriptionCollector.collectEntities called");

        if (this.snsClient == null)
        {
            this.snsClient = AwsClientFactory.getInstance().getSnsClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListSubscriptionsRequest.Builder requestBuilder = ListSubscriptionsRequest.builder();

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("SnsSubscriptionCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListSubscriptionsResponse response = this.snsClient.listSubscriptions(requestBuilder.build());
            List<Subscription> subscriptions = response.subscriptions();
            String             nextToken     = response.nextToken();

            log.debug("SnsSubscriptionCollector received {} subscriptions, nextToken={}",
                subscriptions != null ? subscriptions.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subscriptions != null)
            {
                for (Subscription sub : subscriptions)
                {
                    if (sub == null) continue;

                    String subscriptionArn = sub.subscriptionArn();
                    String topicArn        = sub.topicArn();
                    String protocol        = sub.protocol();
                    String endpoint        = sub.endpoint();
                    String owner           = sub.owner();

                    // Pending confirmation subscriptions have a special ARN value
                    String entityId = (subscriptionArn != null && !subscriptionArn.equals("PendingConfirmation"))
                        ? subscriptionArn
                        : "arn:aws:sns:" + region + ":" + accountId + ":subscription:pending:" + topicArn + ":" + endpoint;

                    String displayName = protocol + ":" + endpoint;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("subscriptionArn", subscriptionArn);
                    attributes.put("topicArn",        topicArn);
                    attributes.put("protocol",        protocol);
                    attributes.put("endpoint",        endpoint);
                    attributes.put("owner",           owner);
                    attributes.put("accountId",       accountId);
                    attributes.put("region",          region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(entityId)
                            .qualifiedResourceName(entityId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(displayName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(sub)
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
            log.error("SnsSubscriptionCollector SNS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SNS subscriptions", e);
        }
        catch (Exception e)
        {
            log.error("SnsSubscriptionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SNS subscriptions", e);
        }
    }
}
