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
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.EventBus;
import software.amazon.awssdk.services.eventbridge.model.ListEventBusesRequest;
import software.amazon.awssdk.services.eventbridge.model.ListEventBusesResponse;

/**
 * Collects EventBridge event buses via the EventBridge ListEventBuses API.
 */
@Slf4j
public class EventBridgeBusCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EventBridgeBus";


    public EventBridgeBusCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "eventbridge", "events", "aws")).build());
        log.debug("EventBridgeBusCollector created");
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
        log.debug("EventBridgeBusCollector.collectEntities called");

        EventBridgeClient eventBridgeClient = AwsClientFactory.getInstance().getEventBridgeClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListEventBusesRequest.Builder requestBuilder = ListEventBusesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EventBridgeBusCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListEventBusesResponse response = eventBridgeClient.listEventBuses(requestBuilder.build());
            List<EventBus> buses    = response.eventBuses();
            String         nextToken = response.nextToken();

            log.debug("EventBridgeBusCollector received {} event buses, nextToken={}",
                buses != null ? buses.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (buses != null)
            {
                for (EventBus bus : buses)
                {
                    if (bus == null) continue;

                    String busArn  = bus.arn();
                    String busName = bus.name();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("eventBusName",  busName);
                    attributes.put("eventBusArn",   busArn);
                    attributes.put("accountId",     accountId);
                    attributes.put("region",        region);
                    attributes.put("policy",        bus.policy());
                    attributes.put("creationTime",
                        bus.creationTime() != null ? bus.creationTime().toString() : null);
                    attributes.put("lastModifiedTime",
                        bus.lastModifiedTime() != null ? bus.lastModifiedTime().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(busArn)
                            .qualifiedResourceName(busArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(busName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(bus)
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
        catch (EventBridgeException e)
        {
            log.error("EventBridgeBusCollector EventBridge error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EventBridge event buses", e);
        }
        catch (Exception e)
        {
            log.error("EventBridgeBusCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EventBridge event buses", e);
        }
    }
}
