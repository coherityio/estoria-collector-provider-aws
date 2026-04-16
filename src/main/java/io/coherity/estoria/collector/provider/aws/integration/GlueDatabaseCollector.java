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
import software.amazon.awssdk.services.glue.model.GlueException;

/**
 * Collects Glue Data Catalog databases via the Glue GetDatabases API.
 */
@Slf4j
public class GlueDatabaseCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "GlueDatabase";


    public GlueDatabaseCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "glue", "data-catalog", "aws")).build());
        log.debug("GlueDatabaseCollector created");
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
        log.debug("GlueDatabaseCollector.collectEntities called");

        GlueClient glueClient = AwsClientFactory.getInstance().getGlueClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetDatabasesRequest.Builder requestBuilder = GetDatabasesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("GlueDatabaseCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            GetDatabasesResponse response = glueClient.getDatabases(requestBuilder.build());
            List<Database> databases = response.databaseList();
            String         nextToken = response.nextToken();

            log.debug("GlueDatabaseCollector received {} databases, nextToken={}",
                databases != null ? databases.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (databases != null)
            {
                for (Database db : databases)
                {
                    if (db == null) continue;

                    String dbName = db.name();
                    // Glue database ARN format: arn:aws:glue:<region>:<accountId>:database/<name>
                    String dbArn  = db.catalogId() != null
                        ? "arn:aws:glue:" + region + ":" + db.catalogId() + ":database/" + dbName
                        : "arn:aws:glue:" + region + ":" + accountId + ":database/" + dbName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("databaseName",    dbName);
                    attributes.put("databaseArn",     dbArn);
                    attributes.put("catalogId",        db.catalogId());
                    attributes.put("description",      db.description());
                    attributes.put("locationUri",      db.locationUri());
                    attributes.put("createTime",
                        db.createTime() != null ? db.createTime().toString() : null);
                    attributes.put("parameters",       db.parameters());
                    attributes.put("accountId",        accountId);
                    attributes.put("region",           region);
                    attributes.put("federatedDatabase",
                        db.federatedDatabase() != null ? db.federatedDatabase().identifier() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(dbArn)
                            .qualifiedResourceName(dbArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(dbName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(db)
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
        catch (GlueException e)
        {
            log.error("GlueDatabaseCollector Glue error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Glue databases", e);
        }
        catch (Exception e)
        {
            log.error("GlueDatabaseCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Glue databases", e);
        }
    }
}
