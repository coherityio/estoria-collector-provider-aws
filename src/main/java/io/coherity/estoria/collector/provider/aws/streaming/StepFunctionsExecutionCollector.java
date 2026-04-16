package io.coherity.estoria.collector.provider.aws.streaming;

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
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.ExecutionListItem;
import software.amazon.awssdk.services.sfn.model.ListExecutionsRequest;
import software.amazon.awssdk.services.sfn.model.ListExecutionsResponse;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesRequest;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesResponse;
import software.amazon.awssdk.services.sfn.model.SfnException;
import software.amazon.awssdk.services.sfn.model.StateMachineListItem;

/**
 * Collects Step Functions execution summaries across all state machines
 * via the SFN ListExecutions API.
 */
@Slf4j
public class StepFunctionsExecutionCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "StepFunctionsExecution";


    public StepFunctionsExecutionCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "stepfunctions", "execution", "workflow", "aws")).build());
        log.debug("StepFunctionsExecutionCollector created");
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
        log.debug("StepFunctionsExecutionCollector.collectEntities called");

        SfnClient sfnClient = AwsClientFactory.getInstance().getSfnClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            int pageSize = collectorRequestParams.getPageSize() > 0 ? collectorRequestParams.getPageSize() : 100;

            // Enumerate all state machines
            List<String> stateMachineArns = new ArrayList<>();
            String smCursor = null;
            do
            {
                ListStateMachinesRequest.Builder smReq = ListStateMachinesRequest.builder().maxResults(100);
                if (smCursor != null) smReq.nextToken(smCursor);
                ListStateMachinesResponse smRes = sfnClient.listStateMachines(smReq.build());
                if (smRes.stateMachines() != null)
                {
                    smRes.stateMachines().forEach(m -> { if (m.stateMachineArn() != null) stateMachineArns.add(m.stateMachineArn()); });
                }
                smCursor = smRes.nextToken();
            }
            while (smCursor != null && !smCursor.isBlank());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String stateMachineArn : stateMachineArns)
            {
                String execCursor = null;
                do
                {
                    ListExecutionsRequest.Builder execReq = ListExecutionsRequest.builder()
                        .stateMachineArn(stateMachineArn)
                        .maxResults(pageSize);
                    if (execCursor != null) execReq.nextToken(execCursor);

                    ListExecutionsResponse execRes = sfnClient.listExecutions(execReq.build());
                    List<ExecutionListItem> executions = execRes.executions();

                    if (executions != null)
                    {
                        for (ExecutionListItem exec : executions)
                        {
                            if (exec == null) continue;

                            String execArn  = exec.executionArn();
                            String execName = exec.name();

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("executionArn",      execArn);
                            attributes.put("executionName",     execName);
                            attributes.put("stateMachineArn",   stateMachineArn);
                            attributes.put("accountId",         accountId);
                            attributes.put("region",            region);
                            attributes.put("status",            exec.statusAsString());
                            attributes.put("startDate",
                                exec.startDate() != null ? exec.startDate().toString() : null);
                            attributes.put("stopDate",
                                exec.stopDate() != null ? exec.stopDate().toString() : null);

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(execArn)
                                    .qualifiedResourceName(execArn)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(execName)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(exec)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }

                    execCursor = execRes.nextToken();
                }
                while (execCursor != null && !execCursor.isBlank());
            }

            log.debug("StepFunctionsExecutionCollector collected {} executions across {} state machines",
                entities.size(), stateMachineArns.size());

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
        catch (SfnException e)
        {
            log.error("StepFunctionsExecutionCollector SFN error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Step Functions executions", e);
        }
        catch (Exception e)
        {
            log.error("StepFunctionsExecutionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Step Functions executions", e);
        }
    }
}
