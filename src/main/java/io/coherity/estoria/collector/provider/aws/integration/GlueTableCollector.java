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
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;

/**
 * Collects Glue Data Catalog tables across all databases via the Glue GetTables API.
 */
@Slf4j
public class GlueTableCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "GlueTable";

    private GlueClient glueClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of(GlueDatabaseCollector.ENTITY_TYPE))
            .tags(Set.of("integration", "glue", "data-catalog", "aws"))
            .build();

    public GlueTableCollector()
    {
        log.debug("GlueTableCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
        log.debug("GlueTableCollector.collectEntities called");

        if (this.glueClient == null)
        {
            this.glueClient = AwsClientFactory.getInstance().getGlueClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // First enumerate all databases
            List<String> dbNames = new ArrayList<>();
            String dbNextToken   = null;
            do
            {
                GetDatabasesRequest.Builder dbReq = GetDatabasesRequest.builder();
                if (dbNextToken != null) dbReq.nextToken(dbNextToken);
                GetDatabasesResponse dbResp = this.glueClient.getDatabases(dbReq.build());
                if (dbResp.databaseList() != null)
                {
                    for (Database db : dbResp.databaseList())
                    {
                        if (db != null && db.name() != null) dbNames.add(db.name());
                    }
                }
                dbNextToken = dbResp.nextToken();
            }
            while (dbNextToken != null && !dbNextToken.isBlank());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            int pageSize = collectorRequestParams.getPageSize();

            for (String dbName : dbNames)
            {
                String tableNextToken = null;
                do
                {
                    GetTablesRequest.Builder reqBuilder = GetTablesRequest.builder().databaseName(dbName);
                    if (pageSize > 0)   reqBuilder.maxResults(pageSize);
                    if (tableNextToken != null) reqBuilder.nextToken(tableNextToken);

                    GetTablesResponse tabResp = this.glueClient.getTables(reqBuilder.build());
                    tableNextToken = tabResp.nextToken();

                    List<Table> tables = tabResp.tableList();
                    if (tables != null)
                    {
                        for (Table table : tables)
                        {
                            if (table == null) continue;

                            String tableName = table.name();
                            // ARN: arn:aws:glue:<region>:<accountId>:table/<db>/<table>
                            String tableArn  = "arn:aws:glue:" + region + ":" + accountId
                                + ":table/" + dbName + "/" + tableName;

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("tableName",     tableName);
                            attributes.put("tableArn",      tableArn);
                            attributes.put("databaseName",  dbName);
                            attributes.put("description",   table.description());
                            attributes.put("tableType",     table.tableType());
                            attributes.put("owner",         table.owner());
                            attributes.put("createTime",
                                table.createTime() != null ? table.createTime().toString() : null);
                            attributes.put("updateTime",
                                table.updateTime() != null ? table.updateTime().toString() : null);
                            attributes.put("lastAccessTime",
                                table.lastAccessTime() != null ? table.lastAccessTime().toString() : null);
                            attributes.put("parameters",    table.parameters());
                            attributes.put("accountId",     accountId);
                            attributes.put("region",        region);

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(tableArn)
                                    .qualifiedResourceName(tableArn)
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
                }
                while (tableNextToken != null && !tableNextToken.isBlank());
            }

            log.debug("GlueTableCollector collected {} tables across {} databases",
                entities.size(), dbNames.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());
            metadataValues.put("databaseCount", dbNames.size());

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
        catch (GlueException e)
        {
            log.error("GlueTableCollector Glue error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Glue tables", e);
        }
        catch (Exception e)
        {
            log.error("GlueTableCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Glue tables", e);
        }
    }
}
