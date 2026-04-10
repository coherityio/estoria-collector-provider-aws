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
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.ListPoliciesRequest;
import software.amazon.awssdk.services.organizations.model.ListPoliciesResponse;
import software.amazon.awssdk.services.organizations.model.ListTargetsForPolicyRequest;
import software.amazon.awssdk.services.organizations.model.ListTargetsForPolicyResponse;
import software.amazon.awssdk.services.organizations.model.OrganizationsException;
import software.amazon.awssdk.services.organizations.model.PolicySummary;
import software.amazon.awssdk.services.organizations.model.PolicyTargetSummary;
import software.amazon.awssdk.services.organizations.model.PolicyType;

/**
 * Collects AWS Organizations policy attachments (targets for each policy).
 * For every SCP/TagPolicy/BackupPolicy found, lists all attached targets.
 */
@Slf4j
public class OrganizationsPolicyAttachmentCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "OrganizationsPolicyAttachment";
    private static final int PAGE_SIZE = 20;

    private OrganizationsClient organizationsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "organizations", "aws"))
            .build();

    public OrganizationsPolicyAttachmentCollector()
    {
        log.debug("OrganizationsPolicyAttachmentCollector created");
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
        log.debug("OrganizationsPolicyAttachmentCollector.collect called");

        if (this.organizationsClient == null)
        {
            this.organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            PolicyType[] policyTypes = {
                PolicyType.SERVICE_CONTROL_POLICY,
                PolicyType.TAG_POLICY,
                PolicyType.BACKUP_POLICY,
                PolicyType.AISERVICES_OPT_OUT_POLICY
            };

            // Collect all policies first
            List<PolicySummary> allPolicies = new ArrayList<>();
            for (PolicyType policyType : policyTypes)
            {
                String nextToken = null;
                do
                {
                    try
                    {
                        ListPoliciesResponse listResponse = this.organizationsClient.listPolicies(
                            ListPoliciesRequest.builder()
                                .filter(policyType)
                                .maxResults(PAGE_SIZE)
                                .nextToken(nextToken)
                                .build());
                        allPolicies.addAll(listResponse.policies());
                        nextToken = listResponse.nextToken();
                    }
                    catch (OrganizationsException e)
                    {
                        if ("PolicyTypeNotEnabledException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
                        {
                            log.debug("Policy type {} not enabled, skipping", policyType);
                            break;
                        }
                        throw e;
                    }
                }
                while (nextToken != null);
            }

            // For each policy, collect attachments
            for (PolicySummary policy : allPolicies)
            {
                String targetsNextToken = null;
                do
                {
                    ListTargetsForPolicyResponse targetsResponse = this.organizationsClient.listTargetsForPolicy(
                        ListTargetsForPolicyRequest.builder()
                            .policyId(policy.id())
                            .maxResults(PAGE_SIZE)
                            .nextToken(targetsNextToken)
                            .build());

                    for (PolicyTargetSummary target : targetsResponse.targets())
                    {
                        String compositeId = policy.id() + "/" + target.targetId();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("policyId", policy.id());
                        attributes.put("policyArn", policy.arn());
                        attributes.put("policyName", policy.name());
                        attributes.put("policyType", policy.typeAsString());
                        attributes.put("targetId", target.targetId());
                        attributes.put("targetName", target.name());
                        attributes.put("targetArn", target.arn());
                        attributes.put("targetType", target.typeAsString());

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(compositeId)
                                .qualifiedResourceName(compositeId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(policy.name() + " -> " + target.name())
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(target)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);
                    }
                    targetsNextToken = targetsResponse.nextToken();
                }
                while (targetsNextToken != null);
            }

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
        catch (OrganizationsException e)
        {
            log.error("OrganizationsPolicyAttachmentCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Organizations policy attachments", e);
        }
        catch (Exception e)
        {
            log.error("OrganizationsPolicyAttachmentCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Organizations policy attachments", e);
        }
    }
}
