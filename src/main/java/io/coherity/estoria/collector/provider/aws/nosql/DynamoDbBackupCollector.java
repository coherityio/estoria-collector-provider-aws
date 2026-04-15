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
import software.amazon.awssdk.services.dynamodb.model.BackupSummary;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ListBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListBackupsResponse;

/**
 * Collects DynamoDB table backups via the DynamoDB ListBackups API.
 */
@Slf4j
public class DynamoDbBackupCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "DynamoDbBackup";

    private DynamoDbClient dynamoDbClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "nosql", "dynamodb", "backup", "aws"))
            .build();

    public DynamoDbBackupCollector()
    {
        log.debug("DynamoDbBackupCollector created");
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
        log.debug("DynamoDbBackupCollector.collectEntities called");

        if (this.dynamoDbClient == null)
        {
            this.dynamoDbClient = AwsClientFactory.getInstance().getDynamoDbClient(providerContext);
        }

        try
        {
            ListBackupsRequest.Builder requestBuilder = ListBackupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("DynamoDbBackupCollector resuming from exclusiveStartBackupArn: {}", token);
                requestBuilder.exclusiveStartBackupArn(token);
            });

            ListBackupsResponse response = this.dynamoDbClient.listBackups(requestBuilder.build());
            List<BackupSummary> backups = response.backupSummaries();
            String lastEvaluated = response.lastEvaluatedBackupArn();

            log.debug("DynamoDbBackupCollector received {} backups, lastEvaluated={}",
                backups != null ? backups.size() : 0, lastEvaluated);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (backups != null)
            {
                for (BackupSummary backup : backups)
                {
                    if (backup == null) continue;

                    String backupArn  = backup.backupArn();
                    String backupName = backup.backupName();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("backupArn", backupArn);
                    attributes.put("backupName", backupName);
                    attributes.put("tableName", backup.tableName());
                    attributes.put("tableArn", backup.tableArn());
                    attributes.put("tableId", backup.tableId());
                    attributes.put("backupStatus", backup.backupStatusAsString());
                    attributes.put("backupType", backup.backupTypeAsString());
                    attributes.put("backupSizeBytes", backup.backupSizeBytes());
                    attributes.put("backupCreationDateTime",
                        backup.backupCreationDateTime() != null ? backup.backupCreationDateTime().toString() : null);
                    attributes.put("backupExpiryDateTime",
                        backup.backupExpiryDateTime() != null ? backup.backupExpiryDateTime().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(backupArn != null ? backupArn : backupName)
                            .qualifiedResourceName(backupArn != null ? backupArn : backupName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(backupName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(backup)
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
            log.error("DynamoDbBackupCollector DynamoDB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect DynamoDB backups", e);
        }
        catch (Exception e)
        {
            log.error("DynamoDbBackupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting DynamoDB backups", e);
        }
    }
}
