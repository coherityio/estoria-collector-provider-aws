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
import software.amazon.awssdk.services.codegurureviewer.CodeGuruReviewerClient;
import software.amazon.awssdk.services.codegurureviewer.model.CodeGuruReviewerException;
import software.amazon.awssdk.services.codegurureviewer.model.ListRepositoryAssociationsRequest;
import software.amazon.awssdk.services.codegurureviewer.model.ListRepositoryAssociationsResponse;
import software.amazon.awssdk.services.codegurureviewer.model.RepositoryAssociationSummary;

@Slf4j
public class CodeGuruReviewerRepositoryAssociationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeGuruReviewerRepositoryAssociation";


    public CodeGuruReviewerRepositoryAssociationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codeguru-reviewer", "repository", "aws")).build());
        log.debug("CodeGuruReviewerRepositoryAssociationCollector created");
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
        CodeGuruReviewerClient codeGuruReviewerClient = AwsClientFactory.getInstance().getCodeGuruReviewerClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListRepositoryAssociationsRequest.Builder requestBuilder = ListRepositoryAssociationsRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListRepositoryAssociationsResponse response = codeGuruReviewerClient.listRepositoryAssociations(requestBuilder.build());
            List<RepositoryAssociationSummary> associations = response.repositoryAssociationSummaries();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (associations != null)
            {
                for (RepositoryAssociationSummary association : associations)
                {
                    if (association == null) continue;

                    String associationId = association.associationId();
                    String associationArn = association.associationArn() != null
                        ? association.associationArn()
                        : ARNHelper.codeGuruReviewerRepositoryAssociationArn(region, accountId, associationId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("associationId", associationId);
                    attributes.put("associationArn", associationArn);
                    attributes.put("name", association.name());
                    attributes.put("owner", association.owner());
                    attributes.put("providerType", association.providerTypeAsString());
                    attributes.put("state", association.stateAsString());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(associationId)
                            .qualifiedResourceName(associationArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(association.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(association)
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
        catch (CodeGuruReviewerException e)
        {
            throw new CollectorException("Failed to collect CodeGuru Reviewer repository associations", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeGuru Reviewer repository associations", e);
        }
    }
}