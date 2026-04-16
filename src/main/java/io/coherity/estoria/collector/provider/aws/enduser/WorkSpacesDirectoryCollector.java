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
import software.amazon.awssdk.services.workspaces.model.DescribeWorkspaceDirectoriesRequest;
import software.amazon.awssdk.services.workspaces.model.DescribeWorkspaceDirectoriesResponse;
import software.amazon.awssdk.services.workspaces.model.WorkspaceDirectory;

@Slf4j
public class WorkSpacesDirectoryCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "WorkSpacesDirectory";


    public WorkSpacesDirectoryCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("enduser", "workspaces", "directory", "aws")).build());
        log.debug("WorkSpacesDirectoryCollector created");
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

            DescribeWorkspaceDirectoriesRequest.Builder requestBuilder = DescribeWorkspaceDirectoriesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeWorkspaceDirectoriesResponse response = workspacesClient.describeWorkspaceDirectories(requestBuilder.build());
            List<WorkspaceDirectory> directories = response.directories();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (directories != null)
            {
                for (WorkspaceDirectory directory : directories)
                {
                    if (directory == null)
                    {
                        continue;
                    }

                    String directoryId = directory.directoryId();
                    String directoryArn = ARNHelper.workSpacesDirectoryArn(region, accountId, directoryId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("directoryId", directoryId);
                    attributes.put("directoryArn", directoryArn);
                    attributes.put("alias", directory.alias());
                    attributes.put("directoryName", directory.directoryName());
                    attributes.put("registrationCode", directory.registrationCode());
                    attributes.put("state", directory.stateAsString());
                    attributes.put("workspaceSecurityGroupId", directory.workspaceSecurityGroupId());
                    attributes.put("iamRoleId", directory.iamRoleId());
                    attributes.put("subnetIds", directory.subnetIds());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(directoryId)
                            .qualifiedResourceName(directoryArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(directory.alias() != null ? directory.alias() : directoryId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(directory)
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
            log.error("WorkSpacesDirectoryCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WorkSpaces directories", e);
        }
    }
}