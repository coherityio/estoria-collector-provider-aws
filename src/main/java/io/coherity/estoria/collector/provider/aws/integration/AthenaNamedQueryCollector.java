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
import software.amazon.awssdk.services.athena.model.BatchGetNamedQueryRequest;
import software.amazon.awssdk.services.athena.model.BatchGetNamedQueryResponse;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesRequest;
import software.amazon.awssdk.services.athena.model.ListNamedQueriesResponse;
import software.amazon.awssdk.services.athena.model.NamedQuery;

/**
 * Collects Athena saved (named) queries via ListNamedQueries + BatchGetNamedQuery APIs.
 */
@Slf4j
public class AthenaNamedQueryCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AthenaNamedQuery";
    private static final int    BATCH_SIZE    = 50; // BatchGetNamedQuery max is 50


    public AthenaNamedQueryCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(AthenaWorkGroupCollector.ENTITY_TYPE), Set.of("integration", "athena", "analytics", "aws")).build());
        log.debug("AthenaNamedQueryCollector created");
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
        log.debug("AthenaNamedQueryCollector.collectEntities called");

        AthenaClient athenaClient = AwsClientFactory.getInstance().getAthenaClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // Step 1: collect all named query IDs (paginated)
            List<String> queryIds  = new ArrayList<>();
            String       nextToken = null;
            do
            {
                ListNamedQueriesRequest.Builder listReq = ListNamedQueriesRequest.builder();
                if (nextToken != null) listReq.nextToken(nextToken);
                ListNamedQueriesResponse listResp = athenaClient.listNamedQueries(listReq.build());
                if (listResp.namedQueryIds() != null) queryIds.addAll(listResp.namedQueryIds());
                nextToken = listResp.nextToken();
            }
            while (nextToken != null && !nextToken.isBlank());

            log.debug("AthenaNamedQueryCollector found {} named query IDs", queryIds.size());

            // Step 2: batch-fetch query details (max 50 per batch)
            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (int i = 0; i < queryIds.size(); i += BATCH_SIZE)
            {
                List<String> batch = queryIds.subList(i, Math.min(i + BATCH_SIZE, queryIds.size()));

                BatchGetNamedQueryResponse batchResp = athenaClient.batchGetNamedQuery(
                    BatchGetNamedQueryRequest.builder().namedQueryIds(batch).build());

                List<NamedQuery> queries = batchResp.namedQueries();
                if (queries != null)
                {
                    for (NamedQuery query : queries)
                    {
                        if (query == null) continue;

                        String queryId   = query.namedQueryId();
                        // Synthetic ARN — Athena named queries don't have native ARNs
                        String queryArn  = "arn:aws:athena:" + region + ":" + accountId
                            + ":namedquery/" + queryId;

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("namedQueryId",   queryId);
                        attributes.put("namedQueryArn",  queryArn);
                        attributes.put("queryName",      query.name());
                        attributes.put("description",    query.description());
                        attributes.put("database",       query.database());
                        attributes.put("workGroup",      query.workGroup());
                        attributes.put("queryString",    query.queryString());
                        attributes.put("accountId",      accountId);
                        attributes.put("region",         region);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(queryArn)
                                .qualifiedResourceName(queryArn)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(query.name() != null ? query.name() : queryId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(query)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                }
            }

            log.debug("AthenaNamedQueryCollector collected {} named queries", entities.size());

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
        catch (AthenaException e)
        {
            log.error("AthenaNamedQueryCollector Athena error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Athena named queries", e);
        }
        catch (Exception e)
        {
            log.error("AthenaNamedQueryCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Athena named queries", e);
        }
    }
}
