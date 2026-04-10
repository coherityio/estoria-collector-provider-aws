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
import software.amazon.awssdk.services.organizations.model.Account;
import software.amazon.awssdk.services.organizations.model.ListAccountsRequest;
import software.amazon.awssdk.services.organizations.model.ListAccountsResponse;
import software.amazon.awssdk.services.ssoadmin.SsoAdminClient;
import software.amazon.awssdk.services.ssoadmin.model.AccountAssignment;
import software.amazon.awssdk.services.ssoadmin.model.InstanceMetadata;
import software.amazon.awssdk.services.ssoadmin.model.ListAccountAssignmentsRequest;
import software.amazon.awssdk.services.ssoadmin.model.ListAccountAssignmentsResponse;
import software.amazon.awssdk.services.ssoadmin.model.ListInstancesRequest;
import software.amazon.awssdk.services.ssoadmin.model.ListInstancesResponse;
import software.amazon.awssdk.services.ssoadmin.model.ListPermissionSetsRequest;
import software.amazon.awssdk.services.ssoadmin.model.ListPermissionSetsResponse;
import software.amazon.awssdk.services.ssoadmin.model.SsoAdminException;

/**
 * Collects AWS IAM Identity Center (SSO) account assignments.
 * For every instance → permission set → account combination, lists assigned principals.
 */
@Slf4j
public class SsoAssignmentCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SsoAssignment";
    private static final int PAGE_SIZE = 100;

    private SsoAdminClient ssoAdminClient;
    private OrganizationsClient organizationsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "sso", "aws"))
            .build();

    public SsoAssignmentCollector()
    {
        log.debug("SsoAssignmentCollector created");
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
        log.debug("SsoAssignmentCollector.collect called");

        if (this.ssoAdminClient == null)
        {
            this.ssoAdminClient = AwsClientFactory.getInstance().getSsoAdminClient(providerContext);
        }
        if (this.organizationsClient == null)
        {
            this.organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // Discover SSO instances
            List<InstanceMetadata> instances = new ArrayList<>();
            String instancesNextToken = null;
            do
            {
                ListInstancesResponse instancesResponse = this.ssoAdminClient.listInstances(
                    ListInstancesRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(instancesNextToken)
                        .build());
                instances.addAll(instancesResponse.instances());
                instancesNextToken = instancesResponse.nextToken();
            }
            while (instancesNextToken != null);

            // Collect account IDs from Organizations
            List<String> accountIds = new ArrayList<>();
            String accountsNextToken = null;
            do
            {
                ListAccountsResponse accountsResponse = this.organizationsClient.listAccounts(
                    ListAccountsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(accountsNextToken)
                        .build());
                for (Account account : accountsResponse.accounts())
                {
                    accountIds.add(account.id());
                }
                accountsNextToken = accountsResponse.nextToken();
            }
            while (accountsNextToken != null);

            for (InstanceMetadata instance : instances)
            {
                String instanceArn = instance.instanceArn();

                // List all permission sets for this instance
                List<String> permissionSetArns = new ArrayList<>();
                String psNextToken = null;
                do
                {
                    ListPermissionSetsResponse psResponse = this.ssoAdminClient.listPermissionSets(
                        ListPermissionSetsRequest.builder()
                            .instanceArn(instanceArn)
                            .maxResults(PAGE_SIZE)
                            .nextToken(psNextToken)
                            .build());
                    permissionSetArns.addAll(psResponse.permissionSets());
                    psNextToken = psResponse.nextToken();
                }
                while (psNextToken != null);

                // For each permission set and each account, list assignments
                for (String permissionSetArn : permissionSetArns)
                {
                    for (String accountId : accountIds)
                    {
                        String assignmentNextToken = null;
                        do
                        {
                            try
                            {
                                ListAccountAssignmentsResponse assignResponse = this.ssoAdminClient.listAccountAssignments(
                                    ListAccountAssignmentsRequest.builder()
                                        .instanceArn(instanceArn)
                                        .accountId(accountId)
                                        .permissionSetArn(permissionSetArn)
                                        .maxResults(PAGE_SIZE)
                                        .nextToken(assignmentNextToken)
                                        .build());

                                for (AccountAssignment assignment : assignResponse.accountAssignments())
                                {
                                    String compositeId = accountId + "/" + permissionSetArn + "/" + assignment.principalId();

                                    Map<String, Object> attributes = new HashMap<>();
                                    attributes.put("instanceArn", instanceArn);
                                    attributes.put("accountId", assignment.accountId());
                                    attributes.put("permissionSetArn", assignment.permissionSetArn());
                                    attributes.put("principalId", assignment.principalId());
                                    attributes.put("principalType", assignment.principalTypeAsString());

                                    CloudEntity entity = CloudEntity.builder()
                                        .entityIdentifier(EntityIdentifier.builder()
                                            .id(compositeId)
                                            .qualifiedResourceName(compositeId)
                                            .build())
                                        .entityType(ENTITY_TYPE)
                                        .name(assignment.principalId())
                                        .collectorContext(collectorContext)
                                        .attributes(attributes)
                                        .rawPayload(assignment)
                                        .collectedAt(Instant.now())
                                        .build();
                                    entities.add(entity);
                                }
                                assignmentNextToken = assignResponse.nextToken();
                            }
                            catch (SsoAdminException e)
                            {
                                // Account may not be in scope — skip
                                log.debug("Skipping account {} for permission set {}: {}", accountId, permissionSetArn,
                                    e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage());
                                break;
                            }
                        }
                        while (assignmentNextToken != null);
                    }
                }
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
        catch (SsoAdminException e)
        {
            log.error("SsoAssignmentCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSO assignments", e);
        }
        catch (Exception e)
        {
            log.error("SsoAssignmentCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSO assignments", e);
        }
    }
}
