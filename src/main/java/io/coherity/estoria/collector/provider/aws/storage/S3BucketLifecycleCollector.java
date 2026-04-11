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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationResponse;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Collects S3 bucket lifecycle configurations.
 */
@Slf4j
public class S3BucketLifecycleCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "S3BucketLifecycle";

    private S3Client s3Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("S3Bucket"))
            .tags(Set.of("storage", "s3", "lifecycle", "aws"))
            .build();

    public S3BucketLifecycleCollector()
    {
        log.debug("S3BucketLifecycleCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("S3BucketLifecycleCollector.collect called");

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

                    GetBucketLifecycleConfigurationResponse lcResponse;
                    try
                    {
                        lcResponse = this.s3Client.getBucketLifecycleConfiguration(
                            GetBucketLifecycleConfigurationRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        String code = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : "";
                        if ("NoSuchLifecycleConfiguration".equals(code))
                        {
                            log.debug("S3BucketLifecycleCollector no lifecycle config for bucket: {}", bucketName);
                            continue;
                        }
                        log.warn("S3BucketLifecycleCollector error for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    if (lcResponse.rules() == null || lcResponse.rules().isEmpty()) continue;

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/lifecycle";

                    List<Map<String, Object>> rules = new ArrayList<>();
                    for (LifecycleRule rule : lcResponse.rules())
                    {
                        Map<String, Object> r = new HashMap<>();
                        r.put("id", rule.id());
                        r.put("status", rule.statusAsString());
                        r.put("prefix", rule.prefix());
                        if (rule.expiration() != null)
                        {
                            r.put("expirationDays", rule.expiration().days());
                            r.put("expirationDate", rule.expiration().date() != null
                                ? rule.expiration().date().toString() : null);
                        }
                        rules.add(r);
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("rules", rules);
                    attributes.put("ruleCount", rules.size());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/lifecycle")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(lcResponse)
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
            log.error("S3BucketLifecycleCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 bucket lifecycle configs", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketLifecycleCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 bucket lifecycle configs", e);
        }
    }
}
