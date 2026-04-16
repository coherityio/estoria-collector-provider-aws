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
import software.amazon.awssdk.services.timestreamwrite.model.ListDatabasesResponse;
import software.amazon.awssdk.services.timestreamwrite.model.TimestreamWriteException;

/**
 * Collects Amazon Timestream databases via the TimestreamWrite ListDatabases API.
 */
@Slf4j
public class TimestreamDatabaseCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "TimestreamDatabase";


    public TimestreamDatabaseCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "timestream", "timeseries", "aws")).build());
        log.debug("TimestreamDatabaseCollector created");
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
        log.debug("TimestreamDatabaseCollector.collectEntities called");

        TimestreamWriteClient timestreamClient = AwsClientFactory.getInstance().getTimestreamWriteClient(providerContext);

        try
        {
            ListDatabasesRequest.Builder requestBuilder = ListDatabasesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("TimestreamDatabaseCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListDatabasesResponse response = timestreamClient.listDatabases(requestBuilder.build());
            List<Database> databases = response.databases();
            String nextToken = response.nextToken();

            log.debug("TimestreamDatabaseCollector received {} databases, nextToken={}",
                databases != null ? databases.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (databases != null)
            {
                for (Database database : databases)
                {
                    if (database == null) continue;

                    String databaseName = database.databaseName();
                    String arn          = database.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("databaseName", databaseName);
                    attributes.put("arn", arn);
                    attributes.put("tableCount", database.tableCount());
                    attributes.put("kmsKeyId", database.kmsKeyId());
                    attributes.put("creationTime",
                        database.creationTime() != null ? database.creationTime().toString() : null);
                    attributes.put("lastUpdatedTime",
                        database.lastUpdatedTime() != null ? database.lastUpdatedTime().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : databaseName)
                            .qualifiedResourceName(arn != null ? arn : databaseName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(databaseName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(database)
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
        catch (TimestreamWriteException e)
        {
            log.error("TimestreamDatabaseCollector Timestream error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Timestream databases", e);
        }
        catch (Exception e)
        {
            log.error("TimestreamDatabaseCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Timestream databases", e);
        }
    }
}
