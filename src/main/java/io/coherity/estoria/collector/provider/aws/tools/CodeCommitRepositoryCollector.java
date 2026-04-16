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
import software.amazon.awssdk.services.codecommit.CodeCommitClient;
import software.amazon.awssdk.services.codecommit.model.CodeCommitException;
import software.amazon.awssdk.services.codecommit.model.ListRepositoriesRequest;
import software.amazon.awssdk.services.codecommit.model.ListRepositoriesResponse;
import software.amazon.awssdk.services.codecommit.model.RepositoryNameIdPair;

@Slf4j
public class CodeCommitRepositoryCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeCommitRepository";


    public CodeCommitRepositoryCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codecommit", "repository", "aws")).build());
        log.debug("CodeCommitRepositoryCollector created");
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
        CodeCommitClient codeCommitClient = AwsClientFactory.getInstance().getCodeCommitClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListRepositoriesRequest.Builder requestBuilder = ListRepositoriesRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListRepositoriesResponse response = codeCommitClient.listRepositories(requestBuilder.build());
            List<RepositoryNameIdPair> repositories = response.repositories();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (repositories != null)
            {
                for (RepositoryNameIdPair repository : repositories)
                {
                    if (repository == null) continue;

                    String repositoryName = repository.repositoryName();
                    String repositoryId = repository.repositoryId();
                    String repositoryArn = ARNHelper.codeCommitRepositoryArn(region, accountId, repositoryName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("repositoryId", repositoryId);
                    attributes.put("repositoryName", repositoryName);
                    attributes.put("repositoryArn", repositoryArn);
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(repositoryId != null ? repositoryId : repositoryName)
                            .qualifiedResourceName(repositoryArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(repositoryName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(repository)
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
        catch (CodeCommitException e)
        {
            throw new CollectorException("Failed to collect CodeCommit repositories", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeCommit repositories", e);
        }
    }
}