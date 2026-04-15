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
import software.amazon.awssdk.services.eventbridge.model.ListEventBusesRequest;
import software.amazon.awssdk.services.eventbridge.model.ListEventBusesResponse;
import software.amazon.awssdk.services.eventbridge.model.EventBus;
import software.amazon.awssdk.services.eventbridge.model.ListRulesRequest;
import software.amazon.awssdk.services.eventbridge.model.ListRulesResponse;
import software.amazon.awssdk.services.eventbridge.model.ListTargetsByRuleRequest;
import software.amazon.awssdk.services.eventbridge.model.ListTargetsByRuleResponse;
import software.amazon.awssdk.services.eventbridge.model.Rule;
import software.amazon.awssdk.services.eventbridge.model.Target;

/**
 * Collects EventBridge rules and their targets via the EventBridge
 * ListRules / ListTargetsByRule APIs across all event buses.
 */
@Slf4j
public class EventBridgeRuleCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "EventBridgeRule";

    private EventBridgeClient eventBridgeClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("streaming", "eventbridge", "events", "rule", "aws"))
            .build();

    public EventBridgeRuleCollector()
    {
        log.debug("EventBridgeRuleCollector created");
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
        log.debug("EventBridgeRuleCollector.collectEntities called");

        if (this.eventBridgeClient == null)
        {
            this.eventBridgeClient = AwsClientFactory.getInstance().getEventBridgeClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // Collect all event bus names first
            List<String> busNames = new ArrayList<>();
            String busesCursor = null;
            do
            {
                ListEventBusesRequest.Builder busReq = ListEventBusesRequest.builder();
                if (busesCursor != null) busReq.nextToken(busesCursor);
                ListEventBusesResponse busRes = this.eventBridgeClient.listEventBuses(busReq.build());
                if (busRes.eventBuses() != null)
                {
                    busRes.eventBuses().forEach(b -> { if (b.name() != null) busNames.add(b.name()); });
                }
                busesCursor = busRes.nextToken();
            }
            while (busesCursor != null && !busesCursor.isBlank());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String busName : busNames)
            {
                String rulesCursor = null;
                do
                {
                    ListRulesRequest.Builder rulesReq = ListRulesRequest.builder().eventBusName(busName);
                    if (rulesCursor != null) rulesReq.nextToken(rulesCursor);

                    ListRulesResponse rulesRes = this.eventBridgeClient.listRules(rulesReq.build());
                    List<Rule> rules = rulesRes.rules();

                    if (rules != null)
                    {
                        for (Rule rule : rules)
                        {
                            if (rule == null) continue;

                            String ruleArn  = rule.arn();
                            String ruleName = rule.name();

                            // Collect targets for this rule
                            List<Map<String, String>> targetList = new ArrayList<>();
                            try
                            {
                                String targetsCursor = null;
                                do
                                {
                                    ListTargetsByRuleRequest.Builder targetReq =
                                        ListTargetsByRuleRequest.builder()
                                            .rule(ruleName)
                                            .eventBusName(busName);
                                    if (targetsCursor != null) targetReq.nextToken(targetsCursor);

                                    ListTargetsByRuleResponse targetsRes =
                                        this.eventBridgeClient.listTargetsByRule(targetReq.build());

                                    if (targetsRes.targets() != null)
                                    {
                                        for (Target t : targetsRes.targets())
                                        {
                                            Map<String, String> tm = new HashMap<>();
                                            tm.put("targetId",  t.id());
                                            tm.put("targetArn", t.arn());
                                            tm.put("roleArn",   t.roleArn());
                                            targetList.add(tm);
                                        }
                                    }
                                    targetsCursor = targetsRes.nextToken();
                                }
                                while (targetsCursor != null && !targetsCursor.isBlank());
                            }
                            catch (Exception ex)
                            {
                                log.warn("EventBridgeRuleCollector could not list targets for rule {}: {}",
                                    ruleName, ex.getMessage());
                            }

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("ruleName",          ruleName);
                            attributes.put("ruleArn",           ruleArn);
                            attributes.put("eventBusName",      busName);
                            attributes.put("accountId",         accountId);
                            attributes.put("region",            region);
                            attributes.put("state",             rule.stateAsString());
                            attributes.put("description",       rule.description());
                            attributes.put("scheduleExpression", rule.scheduleExpression());
                            attributes.put("eventPattern",      rule.eventPattern());
                            attributes.put("roleArn",           rule.roleArn());
                            attributes.put("managedBy",         rule.managedBy());
                            attributes.put("targets",           targetList);
                            attributes.put("targetCount",       targetList.size());

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(ruleArn)
                                    .qualifiedResourceName(ruleArn)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(ruleName)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(rule)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }

                    rulesCursor = rulesRes.nextToken();
                }
                while (rulesCursor != null && !rulesCursor.isBlank());
            }

            log.debug("EventBridgeRuleCollector collected {} rules across {} buses",
                entities.size(), busNames.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (EventBridgeException e)
        {
            log.error("EventBridgeRuleCollector EventBridge error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EventBridge rules", e);
        }
        catch (Exception e)
        {
            log.error("EventBridgeRuleCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EventBridge rules", e);
        }
    }
}
