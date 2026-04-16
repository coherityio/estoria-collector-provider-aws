package io.coherity.estoria.collector.provider.aws.enduser;

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
import software.amazon.awssdk.services.workspaces.WorkSpacesClient;
import software.amazon.awssdk.services.workspaces.model.DescribeWorkspacesRequest;
import software.amazon.awssdk.services.workspaces.model.DescribeWorkspacesResponse;
import software.amazon.awssdk.services.workspaces.model.Workspace;

@Slf4j
public class WorkSpacesWorkspaceCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "WorkSpacesWorkspace";


    public WorkSpacesWorkspaceCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("enduser", "workspaces", "workspace", "aws")).build());
        log.debug("WorkSpacesWorkspaceCollector created");
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
        WorkSpacesClient workspacesClient = AwsClientFactory.getInstance().getWorkspacesClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeWorkspacesRequest.Builder requestBuilder = DescribeWorkspacesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeWorkspacesResponse response = workspacesClient.describeWorkspaces(requestBuilder.build());
            List<Workspace> workspaces = response.workspaces();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (workspaces != null)
            {
                for (Workspace workspace : workspaces)
                {
                    if (workspace == null)
                    {
                        continue;
                    }

                    String workspaceId = workspace.workspaceId();
                    String workspaceArn = ARNHelper.workSpacesWorkspaceArn(region, accountId, workspaceId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("workspaceId", workspaceId);
                    attributes.put("workspaceArn", workspaceArn);
                    attributes.put("directoryId", workspace.directoryId());
                    attributes.put("userName", workspace.userName());
                    attributes.put("bundleId", workspace.bundleId());
                    attributes.put("subnetId", workspace.subnetId());
                    attributes.put("ipAddress", workspace.ipAddress());
                    attributes.put("computerName", workspace.computerName());
                    attributes.put("state", workspace.stateAsString());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(workspaceId)
                            .qualifiedResourceName(workspaceArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(workspace.userName() != null ? workspace.userName() : workspaceId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(workspace)
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
            log.error("WorkSpacesWorkspaceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WorkSpaces workspaces", e);
        }
    }
}