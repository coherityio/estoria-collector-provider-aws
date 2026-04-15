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
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.DescribeParametersRequest;
import software.amazon.awssdk.services.ssm.model.DescribeParametersResponse;
import software.amazon.awssdk.services.ssm.model.GetServiceSettingRequest;
import software.amazon.awssdk.services.ssm.model.ParameterInlinePolicy;
import software.amazon.awssdk.services.ssm.model.ParameterMetadata;
import software.amazon.awssdk.services.ssm.model.ParameterTier;
import software.amazon.awssdk.services.ssm.model.SsmException;

/**
 * Collects SSM Parameter Store parameter policies (expiration, notification, no-change)
 * for Advanced tier parameters only.
 */
@Slf4j
public class SsmParameterPolicyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SsmParameterPolicy";
    private static final int PAGE_SIZE = 50;

    private SsmClient ssmClient;

    public SsmParameterPolicyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "ssm", "aws")).build());
        log.debug("SsmParameterPolicyCollector created");
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
        log.debug("SsmParameterPolicyCollector.collect called");

        if (this.ssmClient == null)
        {
            this.ssmClient = AwsClientFactory.getInstance().getSsmClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                DescribeParametersResponse response = this.ssmClient.describeParameters(
                    DescribeParametersRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (ParameterMetadata param : response.parameters())
                {
                    // Only Advanced tier parameters can have policies
                    if (param.tier() != ParameterTier.ADVANCED)
                    {
                        continue;
                    }

                    List<ParameterInlinePolicy> policies = param.policies();
                    if (policies == null || policies.isEmpty())
                    {
                        continue;
                    }

                    for (int i = 0; i < policies.size(); i++)
                    {
                        ParameterInlinePolicy policy = policies.get(i);
                        String policyId = param.name() + "/policy/" + i;

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("parameterName", param.name());
                        attributes.put("policyType", policy.policyType());
                        attributes.put("policyStatus", policy.policyStatus());
                        attributes.put("policyText", policy.policyText());

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(policyId)
                                .qualifiedResourceName(policyId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(policyId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(policy)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);
                    }
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
        catch (SsmException e)
        {
            log.error("SsmParameterPolicyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSM parameter policies", e);
        }
        catch (Exception e)
        {
            log.error("SsmParameterPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSM parameter policies", e);
        }
    }
}
