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
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.ListPoliciesRequest;
import software.amazon.awssdk.services.organizations.model.ListPoliciesResponse;
import software.amazon.awssdk.services.organizations.model.OrganizationsException;
import software.amazon.awssdk.services.organizations.model.Policy;
import software.amazon.awssdk.services.organizations.model.PolicySummary;
import software.amazon.awssdk.services.organizations.model.PolicyType;

/**
 * Collects AWS Organizations Service Control Policies (SCPs).
 * Also collects TAG_POLICY, BACKUP_POLICY, and AISERVICES_OPT_OUT_POLICY
 * if enabled in the organization.
 */
@Slf4j
public class OrganizationsPolicyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "OrganizationsPolicy";
    private static final int PAGE_SIZE = 20;


    public OrganizationsPolicyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "organizations", "aws")).build());
        log.debug("OrganizationsPolicyCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MANAGEMENT_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ORGANIZATION; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("OrganizationsPolicyCollector.collect called");

        OrganizationsClient organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            PolicyType[] policyTypes = {
                PolicyType.SERVICE_CONTROL_POLICY,
                PolicyType.TAG_POLICY,
                PolicyType.BACKUP_POLICY,
                PolicyType.AISERVICES_OPT_OUT_POLICY
            };

            for (PolicyType policyType : policyTypes)
            {
                String nextToken = null;
                do
                {
                    try
                    {
                        ListPoliciesResponse response = organizationsClient.listPolicies(
                            ListPoliciesRequest.builder()
                                .filter(policyType)
                                .maxResults(PAGE_SIZE)
                                .nextToken(nextToken)
                                .build());

                        for (PolicySummary summary : response.policies())
                        {
                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("policyId", summary.id());
                            attributes.put("arn", summary.arn());
                            attributes.put("name", summary.name());
                            attributes.put("description", summary.description());
                            attributes.put("type", summary.typeAsString());
                            attributes.put("awsManaged", summary.awsManaged());

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(summary.arn())
                                    .qualifiedResourceName(summary.arn())
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(summary.name())
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(summary)
                                .collectedAt(Instant.now())
                                .build();
                            entities.add(entity);
                        }
                        nextToken = response.nextToken();
                    }
                    catch (OrganizationsException e)
                    {
                        // Policy type may not be enabled — skip gracefully
                        if ("PolicyTypeNotEnabledException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
                        {
                            log.debug("Policy type {} not enabled in this organization, skipping", policyType);
                            break;
                        }
                        throw e;
                    }
                }
                while (nextToken != null);
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
            log.error("OrganizationsPolicyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Organizations policies", e);
        }
        catch (Exception e)
        {
            log.error("OrganizationsPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Organizations policies", e);
        }
    }
}
