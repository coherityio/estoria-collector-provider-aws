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
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListPoliciesRequest;
import software.amazon.awssdk.services.iam.model.ListPoliciesResponse;
import software.amazon.awssdk.services.iam.model.Policy;
import software.amazon.awssdk.services.iam.model.PolicyScopeType;

/**
 * Collects IAM managed policies (AWS-managed and customer-managed) via IAM ListPolicies API.
 */
@Slf4j
public class IamPolicyCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "IamPolicy";

    private IamClient iamClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "iam", "policy", "aws"))
            .build();

    public IamPolicyCollector()
    {
        log.debug("IamPolicyCollector created");
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
        log.debug("IamPolicyCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            // Collect only customer-managed (Local) policies to avoid enumerating thousands of AWS-managed ones
            ListPoliciesRequest.Builder requestBuilder = ListPoliciesRequest.builder()
                .scope(PolicyScopeType.LOCAL);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamPolicyCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListPoliciesResponse response = this.iamClient.listPolicies(requestBuilder.build());
            List<Policy> policies = response.policies();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamPolicyCollector received {} policies, nextMarker={}", policies.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Policy policy : policies)
            {
                if (policy == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("policyId", policy.policyId());
                attributes.put("policyName", policy.policyName());
                attributes.put("arn", policy.arn());
                attributes.put("path", policy.path());
                attributes.put("description", policy.description());
                attributes.put("defaultVersionId", policy.defaultVersionId());
                attributes.put("attachmentCount", policy.attachmentCount());
                attributes.put("isAttachable", policy.isAttachable());
                attributes.put("createDate", policy.createDate() != null ? policy.createDate().toString() : null);
                attributes.put("updateDate", policy.updateDate() != null ? policy.updateDate().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(policy.arn())
                        .qualifiedResourceName(policy.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(policy.policyName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(policy)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
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
        catch (IamException e)
        {
            log.error("IamPolicyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM policies", e);
        }
        catch (Exception e)
        {
            log.error("IamPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM policies", e);
        }
    }
}
