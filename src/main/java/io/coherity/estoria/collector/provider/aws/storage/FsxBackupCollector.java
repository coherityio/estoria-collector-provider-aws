package io.coherity.estoria.collector.provider.aws.storage;

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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.fsx.FSxClient;
import software.amazon.awssdk.services.fsx.model.Backup;
import software.amazon.awssdk.services.fsx.model.DescribeBackupsRequest;
import software.amazon.awssdk.services.fsx.model.DescribeBackupsResponse;
import software.amazon.awssdk.services.fsx.model.Tag;

/**
 * Collects FSx backups via the FSx DescribeBackups API.
 */
@Slf4j
public class FsxBackupCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "FsxBackup";

    private FSxClient fsxClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("FsxFileSystem"))
            .tags(Set.of("storage", "fsx", "backup", "aws"))
            .build();

    public FsxBackupCollector()
    {
        log.debug("FsxBackupCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("FsxBackupCollector.collect called");

        if (this.fsxClient == null)
        {
            this.fsxClient = AwsClientFactory.getInstance().getFsxClient(providerContext);
        }

        try
        {
            DescribeBackupsRequest.Builder requestBuilder = DescribeBackupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("FsxBackupCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeBackupsResponse response = this.fsxClient.describeBackups(requestBuilder.build());
            List<Backup> backups = response.backups();
            String nextToken = response.nextToken();

            log.debug("FsxBackupCollector received {} backups, nextToken={}",
                backups != null ? backups.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (backups != null)
            {
                for (Backup backup : backups)
                {
                    if (backup == null) continue;

                    String backupId  = backup.backupId();
                    String backupArn = backup.resourceARN();

                    Map<String, String> tags = backup.tags() == null ? new HashMap<>()
                        : backup.tags().stream().collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", backupId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("backupId", backupId);
                    attributes.put("backupArn", backupArn);
                    attributes.put("type", backup.typeAsString());
                    attributes.put("lifecycle", backup.lifecycleAsString());
                    attributes.put("ownerId", backup.ownerId());
                    attributes.put("kmsKeyId", backup.kmsKeyId());
                    attributes.put("creationTime", backup.creationTime() != null ? backup.creationTime().toString() : null);
                    if (backup.fileSystem() != null)
                    {
                        attributes.put("fileSystemId", backup.fileSystem().fileSystemId());
                        attributes.put("fileSystemType", backup.fileSystem().fileSystemTypeAsString());
                    }
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(backupArn != null ? backupArn : backupId)
                            .qualifiedResourceName(backupArn != null ? backupArn : backupId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(backup)
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
        catch (AwsServiceException e)
        {
            log.error("FsxBackupCollector FSx error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect FSx backups", e);
        }
        catch (Exception e)
        {
            log.error("FsxBackupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting FSx backups", e);
        }
    }
}
