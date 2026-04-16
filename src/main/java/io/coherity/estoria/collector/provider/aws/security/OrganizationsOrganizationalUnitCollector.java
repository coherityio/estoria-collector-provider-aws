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
import software.amazon.awssdk.services.organizations.model.ListOrganizationalUnitsForParentRequest;
import software.amazon.awssdk.services.organizations.model.ListOrganizationalUnitsForParentResponse;
import software.amazon.awssdk.services.organizations.model.ListRootsRequest;
import software.amazon.awssdk.services.organizations.model.ListRootsResponse;
import software.amazon.awssdk.services.organizations.model.OrganizationalUnit;
import software.amazon.awssdk.services.organizations.model.OrganizationsException;
import software.amazon.awssdk.services.organizations.model.Root;

/**
 * Collects all AWS Organizational Units by recursively traversing the
 * Organizations tree starting from the root(s).
 */
@Slf4j
public class OrganizationsOrganizationalUnitCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "OrganizationsOrganizationalUnit";
    private static final int PAGE_SIZE = 20;


    public OrganizationsOrganizationalUnitCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "organizations", "aws")).build());
        log.debug("OrganizationsOrganizationalUnitCollector created");
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
        log.debug("OrganizationsOrganizationalUnitCollector.collect called");

        OrganizationsClient organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // Step 1: list all roots
            List<String> parentIds = new ArrayList<>();
            String rootNextToken = null;
            do
            {
                ListRootsResponse rootResponse = organizationsClient.listRoots(
                    ListRootsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(rootNextToken)
                        .build());
                for (Root root : rootResponse.roots())
                {
                    parentIds.add(root.id());
                }
                rootNextToken = rootResponse.nextToken();
            }
            while (rootNextToken != null);

            // Step 2: BFS/recursive descent
            List<String> queue = new ArrayList<>(parentIds);
            while (!queue.isEmpty())
            {
                String parentId = queue.remove(0);
                String ouNextToken = null;
                do
                {
                    ListOrganizationalUnitsForParentResponse ouResponse =
                        organizationsClient.listOrganizationalUnitsForParent(
                            ListOrganizationalUnitsForParentRequest.builder()
                                .parentId(parentId)
                                .maxResults(PAGE_SIZE)
                                .nextToken(ouNextToken)
                                .build());

                    for (OrganizationalUnit ou : ouResponse.organizationalUnits())
                    {
                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("ouId", ou.id());
                        attributes.put("arn", ou.arn());
                        attributes.put("name", ou.name());
                        attributes.put("parentId", parentId);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(ou.arn())
                                .qualifiedResourceName(ou.arn())
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(ou.name())
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(ou)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);

                        // enqueue OU for its own children
                        queue.add(ou.id());
                    }
                    ouNextToken = ouResponse.nextToken();
                }
                while (ouNextToken != null);
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
            log.error("OrganizationsOrganizationalUnitCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Organizations organizational units", e);
        }
        catch (Exception e)
        {
            log.error("OrganizationsOrganizationalUnitCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Organizations organizational units", e);
        }
    }
}
