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
import software.amazon.awssdk.services.sfn.model.ListStateMachinesRequest;
import software.amazon.awssdk.services.sfn.model.ListStateMachinesResponse;
import software.amazon.awssdk.services.sfn.model.SfnException;
import software.amazon.awssdk.services.sfn.model.StateMachineListItem;

/**
 * Collects Step Functions state machines via the SFN ListStateMachines API.
 */
@Slf4j
public class StepFunctionsStateMachineCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "StepFunctionsStateMachine";

    private SfnClient sfnClient;

    public StepFunctionsStateMachineCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "stepfunctions", "workflow", "aws")).build());
        log.debug("StepFunctionsStateMachineCollector created");
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
        log.debug("StepFunctionsStateMachineCollector.collectEntities called");

        if (this.sfnClient == null)
        {
            this.sfnClient = AwsClientFactory.getInstance().getSfnClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListStateMachinesRequest.Builder requestBuilder = ListStateMachinesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("StepFunctionsStateMachineCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListStateMachinesResponse response   = this.sfnClient.listStateMachines(requestBuilder.build());
            List<StateMachineListItem> machines  = response.stateMachines();
            String                     nextToken = response.nextToken();

            log.debug("StepFunctionsStateMachineCollector received {} state machines, nextToken={}",
                machines != null ? machines.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (machines != null)
            {
                for (StateMachineListItem machine : machines)
                {
                    if (machine == null) continue;

                    String machineArn  = machine.stateMachineArn();
                    String machineName = machine.name();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("stateMachineName",  machineName);
                    attributes.put("stateMachineArn",   machineArn);
                    attributes.put("accountId",         accountId);
                    attributes.put("region",            region);
                    attributes.put("type",              machine.typeAsString());
                    attributes.put("creationDate",
                        machine.creationDate() != null ? machine.creationDate().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(machineArn)
                            .qualifiedResourceName(machineArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(machineName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(machine)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
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
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (SfnException e)
        {
            log.error("StepFunctionsStateMachineCollector SFN error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Step Functions state machines", e);
        }
        catch (Exception e)
        {
            log.error("StepFunctionsStateMachineCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Step Functions state machines", e);
        }
    }
}
