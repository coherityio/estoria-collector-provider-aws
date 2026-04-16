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
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ListWorkGroupsRequest;
import software.amazon.awssdk.services.athena.model.ListWorkGroupsResponse;
import software.amazon.awssdk.services.athena.model.WorkGroupSummary;

/**
 * Collects Athena workgroups via the Athena ListWorkGroups API.
 */
@Slf4j
public class AthenaWorkGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AthenaWorkGroup";


    public AthenaWorkGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "athena", "analytics", "aws")).build());
        log.debug("AthenaWorkGroupCollector created");
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
        log.debug("AthenaWorkGroupCollector.collectEntities called");

        AthenaClient athenaClient = AwsClientFactory.getInstance().getAthenaClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListWorkGroupsRequest.Builder requestBuilder = ListWorkGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("AthenaWorkGroupCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListWorkGroupsResponse response = athenaClient.listWorkGroups(requestBuilder.build());
            List<WorkGroupSummary> workGroups = response.workGroups();
            String                nextToken   = response.nextToken();

            log.debug("AthenaWorkGroupCollector received {} workgroups, nextToken={}",
                workGroups != null ? workGroups.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (workGroups != null)
            {
                for (WorkGroupSummary wg : workGroups)
                {
                    if (wg == null) continue;

                    String wgName = wg.name();
                    // ARN: arn:aws:athena:<region>:<accountId>:workgroup/<name>
                    String wgArn  = "arn:aws:athena:" + region + ":" + accountId + ":workgroup/" + wgName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("workGroupName",    wgName);
                    attributes.put("workGroupArn",     wgArn);
                    attributes.put("description",      wg.description());
                    attributes.put("state",
                        wg.state() != null ? wg.state().toString() : null);
                    attributes.put("engineVersion",
                        wg.engineVersion() != null ? wg.engineVersion().selectedEngineVersion() : null);
                    attributes.put("creationTime",
                        wg.creationTime() != null ? wg.creationTime().toString() : null);
                    attributes.put("accountId",        accountId);
                    attributes.put("region",           region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(wgArn)
                            .qualifiedResourceName(wgArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(wgName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(wg)
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
        catch (AthenaException e)
        {
            log.error("AthenaWorkGroupCollector Athena error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Athena workgroups", e);
        }
        catch (Exception e)
        {
            log.error("AthenaWorkGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Athena workgroups", e);
        }
    }
}
