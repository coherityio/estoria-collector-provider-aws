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
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Collects DynamoDB tables (with throughput and billing details) via the
 * DynamoDB ListTables + DescribeTable APIs.
 */
@Slf4j
public class DynamoDbTableCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "DynamoDbTable";


    public DynamoDbTableCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "nosql", "dynamodb", "aws")).build());
        log.debug("DynamoDbTableCollector created");
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
        log.debug("DynamoDbTableCollector.collectEntities called");

        DynamoDbClient dynamoDbClient = AwsClientFactory.getInstance().getDynamoDbClient(providerContext);

        try
        {
            ListTablesRequest.Builder requestBuilder = ListTablesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("DynamoDbTableCollector resuming from exclusiveStartTableName: {}", token);
                requestBuilder.exclusiveStartTableName(token);
            });

            ListTablesResponse listResponse = dynamoDbClient.listTables(requestBuilder.build());
            List<String> tableNames = listResponse.tableNames();
            String lastEvaluatedTableName = listResponse.lastEvaluatedTableName();

            log.debug("DynamoDbTableCollector received {} table names, lastEvaluatedTableName={}",
                tableNames != null ? tableNames.size() : 0, lastEvaluatedTableName);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (tableNames != null)
            {
                for (String tableName : tableNames)
                {
                    if (tableName == null) continue;

                    try
                    {
                        TableDescription table = dynamoDbClient
                            .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                            .table();

                        if (table == null) continue;

                        String tableArn = table.tableArn();
                        String tableId  = table.tableId();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("tableName", table.tableName());
                        attributes.put("tableArn", tableArn);
                        attributes.put("tableId", tableId);
                        attributes.put("tableStatus", table.tableStatusAsString());
                        attributes.put("billingModeSummary",
                            table.billingModeSummary() != null ? table.billingModeSummary().billingModeAsString() : null);
                        attributes.put("itemCount", table.itemCount());
                        attributes.put("tableSizeBytes", table.tableSizeBytes());
                        attributes.put("streamEnabled",
                            table.streamSpecification() != null && Boolean.TRUE.equals(table.streamSpecification().streamEnabled()));
                        attributes.put("streamViewType",
                            table.streamSpecification() != null ? table.streamSpecification().streamViewTypeAsString() : null);
                        attributes.put("globalSecondaryIndexCount",
                            table.globalSecondaryIndexes() != null ? table.globalSecondaryIndexes().size() : 0);
                        attributes.put("localSecondaryIndexCount",
                            table.localSecondaryIndexes() != null ? table.localSecondaryIndexes().size() : 0);
                        attributes.put("creationDateTime",
                            table.creationDateTime() != null ? table.creationDateTime().toString() : null);
                        if (table.provisionedThroughput() != null)
                        {
                            attributes.put("readCapacityUnits", table.provisionedThroughput().readCapacityUnits());
                            attributes.put("writeCapacityUnits", table.provisionedThroughput().writeCapacityUnits());
                        }

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(tableArn != null ? tableArn : tableName)
                                .qualifiedResourceName(tableArn != null ? tableArn : tableName)
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
                    catch (Exception describeEx)
                    {
                        log.warn("DynamoDbTableCollector failed to describe table {}: {}", tableName, describeEx.getMessage());
                    }
                }
            }

            String finalLastEvaluated = lastEvaluatedTableName;
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
            log.error("DynamoDbTableCollector DynamoDB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect DynamoDB tables", e);
        }
        catch (Exception e)
        {
            log.error("DynamoDbTableCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting DynamoDB tables", e);
        }
    }
}
