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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.wafv2.Wafv2Client;
import software.amazon.awssdk.services.wafv2.model.IPSetSummary;
import software.amazon.awssdk.services.wafv2.model.ListIpSetsRequest;
import software.amazon.awssdk.services.wafv2.model.ListIpSetsResponse;
import software.amazon.awssdk.services.wafv2.model.Scope;

/**
 * Collects AWS WAFv2 IP Sets (REGIONAL scope).
 */
@Slf4j
public class WafIpSetCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "WafIpSet";

    private Wafv2Client wafv2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "waf", "aws"))
            .build();

    public WafIpSetCollector()
    {
        log.debug("WafIpSetCollector created");
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
        log.debug("WafIpSetCollector.collect called");

        if (this.wafv2Client == null)
        {
            this.wafv2Client = AwsClientFactory.getInstance().getWafv2Client(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextMarker = null;

            do
            {
                ListIpSetsRequest.Builder wafRequestBuilder = ListIpSetsRequest.builder()
                    .scope(Scope.REGIONAL)
                    .limit(100);
                if (nextMarker != null)
                {
                    wafRequestBuilder.nextMarker(nextMarker);
                }
                ListIpSetsResponse response = this.wafv2Client.listIPSets(wafRequestBuilder.build());

                for (IPSetSummary ipSet : response.ipSets())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", ipSet.id());
                    attributes.put("name", ipSet.name());
                    attributes.put("arn", ipSet.arn());
                    attributes.put("description", ipSet.description());
                    attributes.put("lockToken", ipSet.lockToken());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(ipSet.arn())
                            .qualifiedResourceName(ipSet.arn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(ipSet.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(ipSet)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextMarker = response.nextMarker();
            }
            while (nextMarker != null);

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
        catch (AwsServiceException e)
        {
            log.error("WafIpSetCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect WAFv2 IP sets", e);
        }
        catch (Exception e)
        {
            log.error("WafIpSetCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WAFv2 IP sets", e);
        }
    }
}
