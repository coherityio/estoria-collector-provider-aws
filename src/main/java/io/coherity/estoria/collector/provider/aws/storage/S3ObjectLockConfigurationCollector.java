package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.provider.aws.ARNHelper;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ObjectLockConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Collects S3 object lock configuration for all buckets.
 */
@Slf4j
public class S3ObjectLockConfigurationCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "S3ObjectLockConfiguration";

    private S3Client s3Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("S3Bucket"))
            .tags(Set.of("storage", "s3", "object-lock", "aws"))
            .build();

    public S3ObjectLockConfigurationCollector()
    {
        log.debug("S3ObjectLockConfigurationCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("S3ObjectLockConfigurationCollector.collect called");

        if (this.s3Client == null)
        {
            this.s3Client = AwsClientFactory.getInstance().getS3Client(providerContext);
        }

        try
        {
            ListBucketsResponse listResponse = this.s3Client.listBuckets();
            List<Bucket> buckets = listResponse.buckets();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (buckets != null)
            {
                for (Bucket bucket : buckets)
                {
                    if (bucket == null) continue;

                    String bucketName = bucket.name();

                    GetObjectLockConfigurationResponse lockResponse;
                    try
                    {
                        lockResponse = this.s3Client.getObjectLockConfiguration(
                            GetObjectLockConfigurationRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        String code = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : "";
                        if ("ObjectLockConfigurationNotFoundError".equals(code)
                            || "NoSuchObjectLockConfiguration".equals(code))
                        {
                            log.debug("S3ObjectLockConfigurationCollector no lock config for bucket: {}", bucketName);
                            continue;
                        }
                        log.warn("S3ObjectLockConfigurationCollector error for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    ObjectLockConfiguration lockConfig = lockResponse.objectLockConfiguration();
                    if (lockConfig == null) continue;

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/object-lock";

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("objectLockEnabled", lockConfig.objectLockEnabledAsString());
                    if (lockConfig.rule() != null && lockConfig.rule().defaultRetention() != null)
                    {
                        attributes.put("defaultRetentionMode",
                            lockConfig.rule().defaultRetention().modeAsString());
                        attributes.put("defaultRetentionDays", lockConfig.rule().defaultRetention().days());
                        attributes.put("defaultRetentionYears", lockConfig.rule().defaultRetention().years());
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/object-lock")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(lockResponse)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (S3Exception e)
        {
            log.error("S3ObjectLockConfigurationCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 object lock configurations", e);
        }
        catch (Exception e)
        {
            log.error("S3ObjectLockConfigurationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 object lock configurations", e);
        }
    }
}
