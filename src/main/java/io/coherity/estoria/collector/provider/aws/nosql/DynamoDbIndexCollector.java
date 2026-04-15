package io.coherity.estoria.collector.provider.aws.nosql;

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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Collects DynamoDB secondary indexes (both GSI and LSI) as discrete entities.
 * Iterates all tables and emits one entity per index found.
 */
@Slf4j
public class DynamoDbIndexCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "DynamoDbIndex";

    private DynamoDbClient dynamoDbClient;

    public DynamoDbIndexCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "nosql", "dynamodb", "index", "aws")).build());
        log.debug("DynamoDbIndexCollector created");
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
        log.debug("DynamoDbIndexCollector.collectEntities called");

        if (this.dynamoDbClient == null)
        {
            this.dynamoDbClient = AwsClientFactory.getInstance().getDynamoDbClient(providerContext);
        }

        try
        {
            ListTablesRequest.Builder requestBuilder = ListTablesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("DynamoDbIndexCollector resuming from exclusiveStartTableName: {}", token);
                requestBuilder.exclusiveStartTableName(token);
            });

            ListTablesResponse listResponse = this.dynamoDbClient.listTables(requestBuilder.build());
            List<String> tableNames = listResponse.tableNames();
            String lastEvaluated = listResponse.lastEvaluatedTableName();

            log.debug("DynamoDbIndexCollector scanning {} tables for indexes, lastEvaluated={}",
                tableNames != null ? tableNames.size() : 0, lastEvaluated);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (tableNames != null)
            {
                for (String tableName : tableNames)
                {
                    if (tableName == null) continue;

                    try
                    {
                        TableDescription table = this.dynamoDbClient
                            .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                            .table();

                        if (table == null) continue;

                        String tableArn = table.tableArn();

                        // Collect GSIs
                        if (table.globalSecondaryIndexes() != null)
                        {
                            for (GlobalSecondaryIndexDescription gsi : table.globalSecondaryIndexes())
                            {
                                if (gsi == null) continue;

                                String indexName = gsi.indexName();
                                String indexArn  = gsi.indexArn();
                                String indexId   = indexArn != null ? indexArn : tableArn + "/index/" + indexName;

                                Map<String, Object> attributes = new HashMap<>();
                                attributes.put("tableName", tableName);
                                attributes.put("tableArn", tableArn);
                                attributes.put("indexName", indexName);
                                attributes.put("indexArn", indexArn);
                                attributes.put("indexType", "GSI");
                                attributes.put("indexStatus", gsi.indexStatusAsString());
                                attributes.put("itemCount", gsi.itemCount());
                                attributes.put("indexSizeBytes", gsi.indexSizeBytes());
                                if (gsi.provisionedThroughput() != null)
                                {
                                    attributes.put("readCapacityUnits", gsi.provisionedThroughput().readCapacityUnits());
                                    attributes.put("writeCapacityUnits", gsi.provisionedThroughput().writeCapacityUnits());
                                }

                                entities.add(CloudEntity.builder()
                                    .entityIdentifier(EntityIdentifier.builder()
                                        .id(indexId)
                                        .qualifiedResourceName(indexId)
                                        .build())
                                    .entityType(ENTITY_TYPE)
                                    .name(tableName + "/" + indexName)
                                    .collectorContext(collectorContext)
                                    .attributes(attributes)
                                    .rawPayload(gsi)
                                    .collectedAt(now)
                                    .build());
                            }
                        }

                        // Collect LSIs
                        if (table.localSecondaryIndexes() != null)
                        {
                            for (LocalSecondaryIndexDescription lsi : table.localSecondaryIndexes())
                            {
                                if (lsi == null) continue;

                                String indexName = lsi.indexName();
                                String indexArn  = lsi.indexArn();
                                String indexId   = indexArn != null ? indexArn : tableArn + "/index/" + indexName;

                                Map<String, Object> attributes = new HashMap<>();
                                attributes.put("tableName", tableName);
                                attributes.put("tableArn", tableArn);
                                attributes.put("indexName", indexName);
                                attributes.put("indexArn", indexArn);
                                attributes.put("indexType", "LSI");
                                attributes.put("itemCount", lsi.itemCount());
                                attributes.put("indexSizeBytes", lsi.indexSizeBytes());

                                entities.add(CloudEntity.builder()
                                    .entityIdentifier(EntityIdentifier.builder()
                                        .id(indexId)
                                        .qualifiedResourceName(indexId)
                                        .build())
                                    .entityType(ENTITY_TYPE)
                                    .name(tableName + "/" + indexName)
                                    .collectorContext(collectorContext)
                                    .attributes(attributes)
                                    .rawPayload(lsi)
                                    .collectedAt(now)
                                    .build());
                            }
                        }
                    }
                    catch (Exception describeEx)
                    {
                        log.warn("DynamoDbIndexCollector failed to describe table {}: {}", tableName, describeEx.getMessage());
                    }
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
            log.error("DynamoDbIndexCollector DynamoDB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect DynamoDB indexes", e);
        }
        catch (Exception e)
        {
            log.error("DynamoDbIndexCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting DynamoDB indexes", e);
        }
    }
}
