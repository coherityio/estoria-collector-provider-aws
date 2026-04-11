package io.coherity.estoria.collector.provider.aws.security;

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
import software.amazon.awssdk.services.config.ConfigClient;
import software.amazon.awssdk.services.config.model.ConfigException;
import software.amazon.awssdk.services.config.model.ConfigRule;
import software.amazon.awssdk.services.config.model.DescribeConfigRulesRequest;
import software.amazon.awssdk.services.config.model.DescribeConfigRulesResponse;

/**
 * Collects AWS Config rules (both managed and custom).
 */
@Slf4j
public class ConfigRuleCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "ConfigRule";
    private static final int PAGE_SIZE = 50;

    private ConfigClient configClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "config", "aws"))
            .build();

    public ConfigRuleCollector()
    {
        log.debug("ConfigRuleCollector created");
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
        log.debug("ConfigRuleCollector.collect called");

        if (this.configClient == null)
        {
            this.configClient = AwsClientFactory.getInstance().getConfigClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                DescribeConfigRulesResponse response = this.configClient.describeConfigRules(
                    DescribeConfigRulesRequest.builder()
                        .nextToken(nextToken)
                        .build());

                for (ConfigRule rule : response.configRules())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("configRuleId", rule.configRuleId());
                    attributes.put("configRuleName", rule.configRuleName());
                    attributes.put("configRuleArn", rule.configRuleArn());
                    attributes.put("description", rule.description());
                    attributes.put("configRuleState", rule.configRuleStateAsString());
                    attributes.put("maximumExecutionFrequency", rule.maximumExecutionFrequencyAsString());
                    attributes.put("inputParameters", rule.inputParameters());
                    if (rule.source() != null)
                    {
                        attributes.put("sourceOwner", rule.source().ownerAsString());
                        attributes.put("sourceIdentifier", rule.source().sourceIdentifier());
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(rule.configRuleArn())
                            .qualifiedResourceName(rule.configRuleArn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(rule.configRuleName())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(rule)
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
        catch (ConfigException e)
        {
            log.error("ConfigRuleCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Config rules", e);
        }
        catch (Exception e)
        {
            log.error("ConfigRuleCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Config rules", e);
        }
    }
}
