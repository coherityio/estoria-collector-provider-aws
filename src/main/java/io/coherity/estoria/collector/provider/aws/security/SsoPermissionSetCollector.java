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
import software.amazon.awssdk.services.ssoadmin.SsoAdminClient;
import software.amazon.awssdk.services.ssoadmin.model.DescribePermissionSetRequest;
import software.amazon.awssdk.services.ssoadmin.model.DescribePermissionSetResponse;
import software.amazon.awssdk.services.ssoadmin.model.InstanceMetadata;
import software.amazon.awssdk.services.ssoadmin.model.ListInstancesRequest;
import software.amazon.awssdk.services.ssoadmin.model.ListInstancesResponse;
import software.amazon.awssdk.services.ssoadmin.model.ListPermissionSetsRequest;
import software.amazon.awssdk.services.ssoadmin.model.ListPermissionSetsResponse;
import software.amazon.awssdk.services.ssoadmin.model.PermissionSet;
import software.amazon.awssdk.services.ssoadmin.model.SsoAdminException;

/**
 * Collects AWS IAM Identity Center (SSO) Permission Sets.
 * Discovers SSO instances first, then lists and describes each permission set.
 */
@Slf4j
public class SsoPermissionSetCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SsoPermissionSet";
    private static final int PAGE_SIZE = 100;

    private SsoAdminClient ssoAdminClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "sso", "aws"))
            .build();

    public SsoPermissionSetCollector()
    {
        log.debug("SsoPermissionSetCollector created");
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
        log.debug("SsoPermissionSetCollector.collect called");

        if (this.ssoAdminClient == null)
        {
            this.ssoAdminClient = AwsClientFactory.getInstance().getSsoAdminClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // Step 1: discover all SSO instances
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

            // Step 2: for each instance, list permission sets and describe each
            for (InstanceMetadata instance : instances)
            {
                String instanceArn = instance.instanceArn();
                String nextToken = null;
                do
                {
                    ListPermissionSetsResponse listResponse = this.ssoAdminClient.listPermissionSets(
                        ListPermissionSetsRequest.builder()
                            .instanceArn(instanceArn)
                            .maxResults(PAGE_SIZE)
                            .nextToken(nextToken)
                            .build());

                    for (String permissionSetArn : listResponse.permissionSets())
                    {
                        DescribePermissionSetResponse describeResponse = this.ssoAdminClient.describePermissionSet(
                            DescribePermissionSetRequest.builder()
                                .instanceArn(instanceArn)
                                .permissionSetArn(permissionSetArn)
                                .build());
                        PermissionSet ps = describeResponse.permissionSet();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("permissionSetArn", ps.permissionSetArn());
                        attributes.put("name", ps.name());
                        attributes.put("description", ps.description());
                        attributes.put("sessionDuration", ps.sessionDuration());
                        attributes.put("relayState", ps.relayState());
                        attributes.put("createdDate", ps.createdDate() != null ? ps.createdDate().toString() : null);
                        attributes.put("instanceArn", instanceArn);
                        attributes.put("identityStoreId", instance.identityStoreId());

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(ps.permissionSetArn())
                                .qualifiedResourceName(ps.permissionSetArn())
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(ps.name())
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(ps)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);
                    }
                    nextToken = listResponse.nextToken();
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
        catch (SsoAdminException e)
        {
            log.error("SsoPermissionSetCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSO permission sets", e);
        }
        catch (Exception e)
        {
            log.error("SsoPermissionSetCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSO permission sets", e);
        }
    }
}
