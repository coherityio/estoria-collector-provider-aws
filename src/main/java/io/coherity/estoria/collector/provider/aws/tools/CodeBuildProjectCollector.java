package io.coherity.estoria.collector.provider.aws.tools;

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
import software.amazon.awssdk.services.codebuild.CodeBuildClient;
import software.amazon.awssdk.services.codebuild.model.BatchGetProjectsRequest;
import software.amazon.awssdk.services.codebuild.model.CodeBuildException;
import software.amazon.awssdk.services.codebuild.model.ListProjectsRequest;
import software.amazon.awssdk.services.codebuild.model.ListProjectsResponse;
import software.amazon.awssdk.services.codebuild.model.Project;

@Slf4j
public class CodeBuildProjectCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeBuildProject";


    public CodeBuildProjectCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codebuild", "ci", "aws")).build());
        log.debug("CodeBuildProjectCollector created");
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
        CodeBuildClient codeBuildClient = AwsClientFactory.getInstance().getCodeBuildClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListProjectsRequest.Builder requestBuilder = ListProjectsRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListProjectsResponse response = codeBuildClient.listProjects(requestBuilder.build());
            List<String> projectNames = response.projects();
            String nextToken = response.nextToken();

            List<Project> projects = projectNames == null || projectNames.isEmpty()
                ? List.of()
                : codeBuildClient.batchGetProjects(BatchGetProjectsRequest.builder().names(projectNames).build()).projects();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Project project : projects)
            {
                if (project == null) continue;

                String projectName = project.name();
                String projectArn = project.arn() != null
                    ? project.arn()
                    : ARNHelper.codeBuildProjectArn(region, accountId, projectName);

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("projectName", projectName);
                attributes.put("projectArn", projectArn);
                attributes.put("description", project.description());
                attributes.put("serviceRole", project.serviceRole());
                attributes.put("sourceType", project.source() != null ? project.source().typeAsString() : null);
                attributes.put("environmentType", project.environment() != null ? project.environment().typeAsString() : null);
                attributes.put("accountId", accountId);
                attributes.put("region", region);

                entities.add(CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(projectName)
                        .qualifiedResourceName(projectArn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(projectName)
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(project)
                    .collectedAt(now)
                    .build());
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
        catch (CodeBuildException e)
        {
            throw new CollectorException("Failed to collect CodeBuild projects", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeBuild projects", e);
        }
    }
}