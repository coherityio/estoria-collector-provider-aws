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
import software.amazon.awssdk.services.wafv2.model.ListRuleGroupsRequest;
import software.amazon.awssdk.services.wafv2.model.ListRuleGroupsResponse;
import software.amazon.awssdk.services.wafv2.model.RuleGroupSummary;
import software.amazon.awssdk.services.wafv2.model.Scope;

/**
 * Collects AWS WAFv2 Rule Groups (REGIONAL scope).
 */
@Slf4j
public class WafRuleGroupCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "WafRuleGroup";

    private Wafv2Client wafv2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "waf", "aws"))
            .build();

    public WafRuleGroupCollector()
    {
        log.debug("WafRuleGroupCollector created");
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
        log.debug("WafRuleGroupCollector.collect called");

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
                ListRuleGroupsRequest.Builder wafRequestBuilder = ListRuleGroupsRequest.builder()
                    .scope(Scope.REGIONAL)
                    .limit(100);
                if (nextMarker != null)
                {
                    wafRequestBuilder.nextMarker(nextMarker);
                }
                ListRuleGroupsResponse response = this.wafv2Client.listRuleGroups(wafRequestBuilder.build());

                for (RuleGroupSummary rg : response.ruleGroups())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", rg.id());
                    attributes.put("name", rg.name());
                    attributes.put("arn", rg.arn());
                    attributes.put("description", rg.description());
                    attributes.put("lockToken", rg.lockToken());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(rg.arn())
                            .qualifiedResourceName(rg.arn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(rg.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(rg)
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
            log.error("WafRuleGroupCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect WAFv2 rule groups", e);
        }
        catch (Exception e)
        {
            log.error("WafRuleGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WAFv2 rule groups", e);
        }
    }
}
