package io.coherity.estoria.collector.provider.aws.integration;

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
import software.amazon.awssdk.services.datapipeline.DataPipelineClient;
import software.amazon.awssdk.services.datapipeline.model.DataPipelineException;
import software.amazon.awssdk.services.datapipeline.model.DescribePipelinesRequest;
import software.amazon.awssdk.services.datapipeline.model.DescribePipelinesResponse;
import software.amazon.awssdk.services.datapipeline.model.Field;
import software.amazon.awssdk.services.datapipeline.model.ListPipelinesRequest;
import software.amazon.awssdk.services.datapipeline.model.ListPipelinesResponse;
import software.amazon.awssdk.services.datapipeline.model.PipelineDescription;
import software.amazon.awssdk.services.datapipeline.model.PipelineIdName;

/**
 * Collects AWS Data Pipeline pipelines via the ListPipelines + DescribePipelines APIs.
 * Note: AWS Data Pipeline is a legacy service.
 */
@Slf4j
public class DataPipelinePipelineCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "DataPipelinePipeline";
    private static final int    DESCRIBE_MAX = 25; // DescribePipelines supports up to 25 IDs per call


    public DataPipelinePipelineCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "data-pipeline", "legacy", "aws")).build());
        log.debug("DataPipelinePipelineCollector created");
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
        log.debug("DataPipelinePipelineCollector.collectEntities called");

        DataPipelineClient dataPipelineClient = AwsClientFactory.getInstance().getDataPipelineClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // Step 1: list all pipeline IDs (paginated by marker)
            List<String> pipelineIds = new ArrayList<>();
            String       marker      = null;
            do
            {
                ListPipelinesRequest.Builder listReq = ListPipelinesRequest.builder();
                if (marker != null) listReq.marker(marker);
                ListPipelinesResponse listResp = dataPipelineClient.listPipelines(listReq.build());
                if (listResp.pipelineIdList() != null)
                {
                    for (PipelineIdName pin : listResp.pipelineIdList())
                    {
                        if (pin != null && pin.id() != null) pipelineIds.add(pin.id());
                    }
                }
                marker = listResp.hasMoreResults() ? listResp.marker() : null;
            }
            while (marker != null && !marker.isBlank());

            log.debug("DataPipelinePipelineCollector found {} pipeline IDs", pipelineIds.size());

            // Step 2: describe pipelines in batches
            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (int i = 0; i < pipelineIds.size(); i += DESCRIBE_MAX)
            {
                List<String> batch = pipelineIds.subList(i, Math.min(i + DESCRIBE_MAX, pipelineIds.size()));

                DescribePipelinesResponse descResp = dataPipelineClient.describePipelines(
                    DescribePipelinesRequest.builder().pipelineIds(batch).build());

                List<PipelineDescription> descriptions = descResp.pipelineDescriptionList();
                if (descriptions != null)
                {
                    for (PipelineDescription pd : descriptions)
                    {
                        if (pd == null) continue;

                        String pipelineId = pd.pipelineId();
                        String pipelineName = pd.name();
                        // Data Pipeline ARN: arn:aws:datapipeline:<region>:<accountId>:pipeline/<id>
                        String pipelineArn  = "arn:aws:datapipeline:" + region + ":" + accountId
                            + ":pipeline/" + pipelineId;

                        // Extract useful fields from the fields list
                        Map<String, String> fieldMap = new HashMap<>();
                        if (pd.fields() != null)
                        {
                            for (Field field : pd.fields())
                            {
                                if (field != null && field.key() != null)
                                {
                                    fieldMap.put(field.key(),
                                        field.stringValue() != null ? field.stringValue() : field.refValue());
                                }
                            }
                        }

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("pipelineId",          pipelineId);
                        attributes.put("pipelineName",        pipelineName);
                        attributes.put("pipelineArn",         pipelineArn);
                        attributes.put("description",         pd.description());
                        attributes.put("uniqueId",            fieldMap.get("uniqueId"));
                        attributes.put("pipelineState",       fieldMap.get("@pipelineState"));
                        attributes.put("createdDate",         fieldMap.get("@creationTime"));
                        attributes.put("lastActivatedDate",   fieldMap.get("@lastActivatedTime"));
                        attributes.put("scheduledPeriod",     fieldMap.get("scheduledPeriod"));
                        attributes.put("tags",                pd.tags());
                        attributes.put("accountId",           accountId);
                        attributes.put("region",              region);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(pipelineArn)
                                .qualifiedResourceName(pipelineArn)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(pipelineName != null ? pipelineName : pipelineId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(pd)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                }
            }

            log.debug("DataPipelinePipelineCollector collected {} pipelines", entities.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (DataPipelineException e)
        {
            log.error("DataPipelinePipelineCollector Data Pipeline error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Data Pipeline pipelines", e);
        }
        catch (Exception e)
        {
            log.error("DataPipelinePipelineCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Data Pipeline pipelines", e);
        }
    }
}
