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
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

/**
 * Collects DynamoDB table streams via the DynamoDB ListTables + DescribeTable APIs.
 * Only tables that have streaming enabled are emitted.
 */
@Slf4j
public class DynamoDbStreamCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "DynamoDbStream";

    private DynamoDbClient dynamoDbClient;

    public DynamoDbStreamCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "nosql", "dynamodb", "stream", "aws")).build());
        log.debug("DynamoDbStreamCollector created");
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
        log.debug("DynamoDbStreamCollector.collectEntities called");

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
                log.debug("DynamoDbStreamCollector resuming from exclusiveStartTableName: {}", token);
                requestBuilder.exclusiveStartTableName(token);
            });

            ListTablesResponse listResponse = this.dynamoDbClient.listTables(requestBuilder.build());
            List<String> tableNames = listResponse.tableNames();
            String lastEvaluated = listResponse.lastEvaluatedTableName();

            log.debug("DynamoDbStreamCollector scanning {} tables for stream configs, lastEvaluated={}",
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

                        StreamSpecification streamSpec = table.streamSpecification();
                        if (streamSpec == null || !Boolean.TRUE.equals(streamSpec.streamEnabled())) continue;

                        String tableArn   = table.tableArn();
                        String latestStreamArn  = table.latestStreamArn();
                        String latestStreamLabel = table.latestStreamLabel();

                        // Use the latestStreamArn as the stream entity id when available
                        String streamId = latestStreamArn != null ? latestStreamArn : tableArn + "/stream";

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("tableName", tableName);
                        attributes.put("tableArn", tableArn);
                        attributes.put("latestStreamArn", latestStreamArn);
                        attributes.put("latestStreamLabel", latestStreamLabel);
                        attributes.put("streamViewType", streamSpec.streamViewTypeAsString());
                        attributes.put("streamEnabled", true);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(streamId)
                                .qualifiedResourceName(streamId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(tableName + "/stream")
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(table)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                    catch (Exception describeEx)
                    {
                        log.warn("DynamoDbStreamCollector failed to describe table {}: {}", tableName, describeEx.getMessage());
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
            log.error("DynamoDbStreamCollector DynamoDB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect DynamoDB streams", e);
        }
        catch (Exception e)
        {
            log.error("DynamoDbStreamCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting DynamoDB streams", e);
        }
    }
}
