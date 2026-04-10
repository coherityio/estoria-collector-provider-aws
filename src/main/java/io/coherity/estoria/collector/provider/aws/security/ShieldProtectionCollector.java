package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.shield.ShieldClient;
import software.amazon.awssdk.services.shield.model.ListProtectionsRequest;
import software.amazon.awssdk.services.shield.model.ListProtectionsResponse;
import software.amazon.awssdk.services.shield.model.Protection;
import software.amazon.awssdk.services.shield.model.ShieldException;

/**
 * Collects AWS Shield Advanced protections.
 * Requires Shield Advanced subscription.
 */
@Slf4j
public class ShieldProtectionCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "ShieldProtection";
    private static final int PAGE_SIZE = 100;

    private ShieldClient shieldClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "shield", "aws"))
            .build();

    public ShieldProtectionCollector()
    {
        log.debug("ShieldProtectionCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("ShieldProtectionCollector.collect called");

        if (this.shieldClient == null)
        {
            this.shieldClient = AwsClientFactory.getInstance().getShieldClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                ListProtectionsResponse response = this.shieldClient.listProtections(
                    ListProtectionsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (Protection protection : response.protections())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("protectionId", protection.id());
                    attributes.put("name", protection.name());
                    attributes.put("resourceArn", protection.resourceArn());
                    attributes.put("protectionArn", protection.protectionArn());
                    attributes.put("healthCheckIds", protection.healthCheckIds());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(protection.protectionArn() != null ? protection.protectionArn() : protection.id())
                            .qualifiedResourceName(protection.protectionArn() != null ? protection.protectionArn() : protection.id())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(protection.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(protection)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextToken = response.nextToken();
            }
            while (nextToken != null);

            final int count = entities.size();
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", count);

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (ShieldException e)
        {
            // Shield Advanced not subscribed — emit empty result gracefully
            if ("ResourceNotFoundException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null)
                || "SubscriptionNotFoundException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
            {
                log.info("Shield Advanced not subscribed, no protections to collect");
                return new CollectorCursor()
                {
                    @Override public List<CloudEntity> getEntities() { return List.of(); }
                    @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                    @Override public CursorMetadata getMetadata()
                    {
                        return CursorMetadata.builder().values(Map.of("count", 0)).build();
                    }
                };
            }
            log.error("ShieldProtectionCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Shield protections", e);
        }
        catch (Exception e)
        {
            log.error("ShieldProtectionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Shield protections", e);
        }
    }
}
