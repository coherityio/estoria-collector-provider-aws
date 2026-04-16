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
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Database;
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesRequest;
import software.amazon.awssdk.services.timestreamwrite.model.ListTablesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.Table;
import software.amazon.awssdk.services.timestreamwrite.model.TimestreamWriteException;

/**
 * Collects Amazon Timestream tables by iterating over all databases via the TimestreamWrite API.
 * Pagination is performed per-database; the cursor token encodes "databaseName:nextToken".
 */
@Slf4j
public class TimestreamTableCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "TimestreamTable";
    private static final String CURSOR_SEP   = "::";


    public TimestreamTableCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "timestream", "timeseries", "aws")).build());
        log.debug("TimestreamTableCollector created");
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
        log.debug("TimestreamTableCollector.collectEntities called");

        TimestreamWriteClient timestreamClient = AwsClientFactory.getInstance().getTimestreamWriteClient(providerContext);

        try
        {
            // Collect all databases first (full list — typically small)
            List<String> databaseNames = new ArrayList<>();
            String dbNextToken = null;
            do {
                ListDatabasesRequest.Builder dbReq = ListDatabasesRequest.builder();
                if (dbNextToken != null) dbReq.nextToken(dbNextToken);
                var dbResp = timestreamClient.listDatabases(dbReq.build());
                if (dbResp.databases() != null)
                {
                    for (Database db : dbResp.databases())
                    {
                        databaseNames.add(db.databaseName());
                    }
                }
                dbNextToken = dbResp.nextToken();
            } while (dbNextToken != null && !dbNextToken.isBlank());

            log.debug("TimestreamTableCollector found {} databases to scan", databaseNames.size());

            // Determine resume position from cursor token (format: "dbName::tableNextToken")
            String resumeDatabaseName = null;
            String resumeTableToken   = null;
            Optional<String> cursorOpt = collectorRequestParams.getCursorToken();
            if (cursorOpt.isPresent())
            {
                String cursor = cursorOpt.get();
                int sepIdx = cursor.indexOf(CURSOR_SEP);
                if (sepIdx >= 0)
                {
                    resumeDatabaseName = cursor.substring(0, sepIdx);
                    String afterSep    = cursor.substring(sepIdx + CURSOR_SEP.length());
                    resumeTableToken   = afterSep.isEmpty() ? null : afterSep;
                }
            }

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();
            int pageSize = collectorRequestParams.getPageSize();

            String nextCursorToken = null;
            boolean resuming = (resumeDatabaseName != null);

            outerLoop:
            for (String databaseName : databaseNames)
            {
                if (resuming && !databaseName.equals(resumeDatabaseName))
                {
                    continue; // skip databases before the resume point
                }

                String tableNextToken = resuming ? resumeTableToken : null;
                resuming = false; // only skip once

                do {
                    ListTablesRequest.Builder tableReq = ListTablesRequest.builder()
                        .databaseName(databaseName);
                    if (pageSize > 0) tableReq.maxResults(pageSize);
                    if (tableNextToken != null) tableReq.nextToken(tableNextToken);

                    ListTablesResponse tableResp = timestreamClient.listTables(tableReq.build());
                    List<Table> tables = tableResp.tables();
                    tableNextToken = tableResp.nextToken();

                    if (tables != null)
                    {
                        for (Table table : tables)
                        {
                            if (table == null) continue;

                            String tableName = table.tableName();
                            String arn       = table.arn();

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("databaseName", databaseName);
                            attributes.put("tableName", tableName);
                            attributes.put("arn", arn);
                            attributes.put("tableStatus", table.tableStatus() != null
                                ? table.tableStatus().toString() : null);
                            attributes.put("creationTime",
                                table.creationTime() != null ? table.creationTime().toString() : null);
                            attributes.put("lastUpdatedTime",
                                table.lastUpdatedTime() != null ? table.lastUpdatedTime().toString() : null);

                            if (table.retentionProperties() != null)
                            {
                                attributes.put("memoryStoreRetentionPeriodInHours",
                                    table.retentionProperties().memoryStoreRetentionPeriodInHours());
                                attributes.put("magneticStoreRetentionPeriodInDays",
                                    table.retentionProperties().magneticStoreRetentionPeriodInDays());
                            }

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(arn != null ? arn : databaseName + "/" + tableName)
                                    .qualifiedResourceName(arn != null ? arn : databaseName + "/" + tableName)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(databaseName + "/" + tableName)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(table)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }

                    // If there are more pages in this database, capture the cursor and stop
                    if (tableNextToken != null && !tableNextToken.isBlank())
                    {
                        nextCursorToken = databaseName + CURSOR_SEP + tableNextToken;
                        break outerLoop;
                    }

                } while (tableNextToken != null && !tableNextToken.isBlank());
            }

            log.debug("TimestreamTableCollector collected {} tables, nextCursorToken={}",
                entities.size(), nextCursorToken);

            String finalNextCursorToken = nextCursorToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextCursorToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (TimestreamWriteException e)
        {
            log.error("TimestreamTableCollector Timestream error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Timestream tables", e);
        }
        catch (Exception e)
        {
            log.error("TimestreamTableCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Timestream tables", e);
        }
    }
}
