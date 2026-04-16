package io.coherity.estoria.collector.provider.aws.monitoring;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.xray.XRayClient;
import software.amazon.awssdk.services.xray.model.GetSamplingRulesRequest;
import software.amazon.awssdk.services.xray.model.GetSamplingRulesResponse;
import software.amazon.awssdk.services.xray.model.SamplingRule;
import software.amazon.awssdk.services.xray.model.SamplingRuleRecord;

@Slf4j
public class XRaySamplingRuleCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "XRaySamplingRule";


    public XRaySamplingRuleCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "xray", "sampling", "rule", "aws")).build());
    }

    @Override public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }
    @Override public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }
    @Override public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        XRayClient xRayClient = AwsClientFactory.getInstance().getXRayClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetSamplingRulesRequest.Builder requestBuilder = GetSamplingRulesRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            GetSamplingRulesResponse response = xRayClient.getSamplingRules(requestBuilder.build());
            List<SamplingRuleRecord> samplingRuleRecords = response.samplingRuleRecords();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (samplingRuleRecords != null)
            {
                for (SamplingRuleRecord samplingRuleRecord : samplingRuleRecords)
                {
                    if (samplingRuleRecord == null)
                    {
                        continue;
                    }

                    SamplingRule samplingRule = samplingRuleRecord.samplingRule();
                    if (samplingRule == null)
                    {
                        continue;
                    }

                    String ruleName = samplingRule.ruleName();
                    String qualifiedName = ARNHelper.xRaySamplingRuleArn(region, accountId, ruleName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("ruleName", ruleName);
                    attributes.put("ruleArn", qualifiedName);
                    attributes.put("priority", samplingRule.priority());
                    attributes.put("fixedRate", samplingRule.fixedRate());
                    attributes.put("reservoirSize", samplingRule.reservoirSize());
                    attributes.put("serviceName", samplingRule.serviceName());
                    attributes.put("serviceType", samplingRule.serviceType());
                    attributes.put("host", samplingRule.host());
                    attributes.put("httpMethod", samplingRule.httpMethod());
                    attributes.put("urlPath", samplingRule.urlPath());
                    attributes.put("version", samplingRule.version());
                    attributes.put("attributes", samplingRule.attributes());
                    attributes.put("createdAt", samplingRuleRecord.createdAt());
                    attributes.put("modifiedAt", samplingRuleRecord.modifiedAt());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(ruleName)
                            .qualifiedResourceName(qualifiedName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(ruleName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(samplingRuleRecord)
                        .collectedAt(now)
                        .build());
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
                    return Optional.ofNullable(finalNextToken).filter(token -> !token.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (Exception e)
        {
            log.error("XRaySamplingRuleCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting X-Ray sampling rules", e);
        }
    }
}