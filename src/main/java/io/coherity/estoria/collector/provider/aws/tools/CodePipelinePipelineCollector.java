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
import software.amazon.awssdk.services.codepipeline.CodePipelineClient;
import software.amazon.awssdk.services.codepipeline.model.CodePipelineException;
import software.amazon.awssdk.services.codepipeline.model.ListPipelinesRequest;
import software.amazon.awssdk.services.codepipeline.model.ListPipelinesResponse;
import software.amazon.awssdk.services.codepipeline.model.PipelineSummary;

@Slf4j
public class CodePipelinePipelineCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodePipelinePipeline";


    public CodePipelinePipelineCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codepipeline", "cd", "aws")).build());
        log.debug("CodePipelinePipelineCollector created");
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
        CodePipelineClient codePipelineClient = AwsClientFactory.getInstance().getCodePipelineClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListPipelinesRequest.Builder requestBuilder = ListPipelinesRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListPipelinesResponse response = codePipelineClient.listPipelines(requestBuilder.build());
            List<PipelineSummary> pipelines = response.pipelines();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (pipelines != null)
            {
                for (PipelineSummary pipeline : pipelines)
                {
                    if (pipeline == null) continue;

                    String pipelineName = pipeline.name();
                    String pipelineArn = ARNHelper.codePipelinePipelineArn(region, accountId, pipelineName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("pipelineName", pipelineName);
                    attributes.put("pipelineArn", pipelineArn);
                    attributes.put("version", pipeline.version());
                    attributes.put("created", pipeline.created() != null ? pipeline.created().toString() : null);
                    attributes.put("updated", pipeline.updated() != null ? pipeline.updated().toString() : null);
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(pipelineName)
                            .qualifiedResourceName(pipelineArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(pipelineName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(pipeline)
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
        catch (CodePipelineException e)
        {
            throw new CollectorException("Failed to collect CodePipeline pipelines", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodePipeline pipelines", e);
        }
    }
}