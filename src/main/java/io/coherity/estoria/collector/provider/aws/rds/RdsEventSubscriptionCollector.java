package io.coherity.estoria.collector.provider.aws.rds;

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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeEventSubscriptionsResponse;
import software.amazon.awssdk.services.rds.model.EventSubscription;
import software.amazon.awssdk.services.rds.model.RdsException;

/**
 * Collects RDS event subscriptions via the RDS DescribeEventSubscriptions API.
 */
@Slf4j
public class RdsEventSubscriptionCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "RdsEventSubscription";

    private RdsClient rdsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "rds", "events", "aws"))
            .build();

    public RdsEventSubscriptionCollector()
    {
        log.debug("RdsEventSubscriptionCollector created");
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
        log.debug("RdsEventSubscriptionCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeEventSubscriptionsRequest.Builder requestBuilder = DescribeEventSubscriptionsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsEventSubscriptionCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeEventSubscriptionsResponse response =
                this.rdsClient.describeEventSubscriptions(requestBuilder.build());
            List<EventSubscription> subscriptions = response.eventSubscriptionsList();
            String nextMarker = response.marker();

            log.debug("RdsEventSubscriptionCollector received {} subscriptions, nextMarker={}",
                subscriptions != null ? subscriptions.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (subscriptions != null)
            {
                for (EventSubscription subscription : subscriptions)
                {
                    if (subscription == null) continue;

                    String subscriptionName = subscription.custSubscriptionId();
                    String arn = subscription.eventSubscriptionArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("custSubscriptionId", subscriptionName);
                    attributes.put("eventSubscriptionArn", arn);
                    attributes.put("customerAwsId", subscription.customerAwsId());
                    attributes.put("snsTopicArn", subscription.snsTopicArn());
                    attributes.put("sourceType", subscription.sourceType());
                    attributes.put("eventCategoriesList", subscription.eventCategoriesList());
                    attributes.put("sourceIdsList", subscription.sourceIdsList());
                    attributes.put("enabled", subscription.enabled());
                    attributes.put("status", subscription.status());
                    attributes.put("subscriptionCreationTime", subscription.subscriptionCreationTime());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : subscriptionName)
                            .qualifiedResourceName(arn != null ? arn : subscriptionName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(subscriptionName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(subscription)
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
            log.error("RdsEventSubscriptionCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS event subscriptions", e);
        }
        catch (Exception e)
        {
            log.error("RdsEventSubscriptionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS event subscriptions", e);
        }
    }
}
