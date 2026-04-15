package io.coherity.estoria.collector.provider.aws.nosql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalTable;
import software.amazon.awssdk.services.dynamodb.model.ListGlobalTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListGlobalTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.Replica;

/**
 * Collects DynamoDB global tables via the DynamoDB ListGlobalTables API.
 */
@Slf4j
public class DynamoDbGlobalTableCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "DynamoDbGlobalTable";

    private DynamoDbClient dynamoDbClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "nosql", "dynamodb", "global", "aws"))
            .build();

    public DynamoDbGlobalTableCollector()
    {
        log.debug("DynamoDbGlobalTableCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("DynamoDbGlobalTableCollector.collectEntities called");

        if (this.dynamoDbClient == null)
        {
            this.dynamoDbClient = AwsClientFactory.getInstance().getDynamoDbClient(providerContext);
        }

        try
        {
            ListGlobalTablesRequest.Builder requestBuilder = ListGlobalTablesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("DynamoDbGlobalTableCollector resuming from exclusiveStartGlobalTableName: {}", token);
                requestBuilder.exclusiveStartGlobalTableName(token);
            });

            ListGlobalTablesResponse response = this.dynamoDbClient.listGlobalTables(requestBuilder.build());
            List<GlobalTable> globalTables = response.globalTables();
            String lastEvaluated = response.lastEvaluatedGlobalTableName();

            log.debug("DynamoDbGlobalTableCollector received {} global tables, lastEvaluated={}",
                globalTables != null ? globalTables.size() : 0, lastEvaluated);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (globalTables != null)
            {
                for (GlobalTable table : globalTables)
                {
                    if (table == null) continue;

                    String tableName = table.globalTableName();

                    // GlobalTable.replicationGroup() returns List<Replica>
                    List<String> replicaRegions = table.replicationGroup() == null ? List.of()
                        : table.replicationGroup().stream()
                            .map(Replica::regionName)
                            .collect(Collectors.toList());

                    // ListGlobalTables does not return an ARN — use a synthetic ID
                    String syntheticId = "dynamodb-global-table:" + tableName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("globalTableName", tableName);
                    attributes.put("replicaRegions", replicaRegions);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(syntheticId)
                            .qualifiedResourceName(syntheticId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(tableName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(table)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalLastEvaluated = lastEvaluated;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalLastEvaluated).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (DynamoDbException e)
        {
            log.error("DynamoDbGlobalTableCollector DynamoDB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect DynamoDB global tables", e);
        }
        catch (Exception e)
        {
            log.error("DynamoDbGlobalTableCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting DynamoDB global tables", e);
        }
    }
}
