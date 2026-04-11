package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryptionRule;

/**
 * Collects S3 buckets including location, versioning, and encryption configuration.
 */
@Slf4j
public class S3BucketCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "S3Bucket";

    private S3Client s3Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("storage", "s3", "aws"))
            .build();

    public S3BucketCollector()
    {
        log.debug("S3BucketCollector created");
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
        log.debug("S3BucketCollector.collect called");

        if (this.s3Client == null)
        {
            this.s3Client = AwsClientFactory.getInstance().getS3Client(providerContext);
        }

        try
        {
            // SDK 2.25.60: ListBuckets does not support server-side pagination—returns all buckets at once
            ListBucketsResponse response = this.s3Client.listBuckets(ListBucketsRequest.builder().build());
            List<Bucket> buckets = response.buckets();
            String nextToken = null;

            log.debug("S3BucketCollector received {} buckets, continuationToken={}",
                buckets != null ? buckets.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (buckets != null)
            {
                for (Bucket bucket : buckets)
                {
                    if (bucket == null) continue;

                    String bucketName = bucket.name();
                    String arn = ARNHelper.s3BucketArn(bucketName);

                    // Resolve location
                    String region = "";
                    try
                    {
                        region = this.s3Client.getBucketLocation(
                            GetBucketLocationRequest.builder().bucket(bucketName).build())
                            .locationConstraintAsString();
                        if (region == null || region.isBlank()) region = "us-east-1";
                    }
                    catch (Exception ex)
                    {
                        log.warn("S3BucketCollector could not get location for bucket {}: {}", bucketName, ex.getMessage());
                    }

                    // Resolve versioning
                    String versioning = "";
                    try
                    {
                        BucketVersioningStatus status = this.s3Client.getBucketVersioning(
                            GetBucketVersioningRequest.builder().bucket(bucketName).build()).status();
                        versioning = status != null ? status.toString() : "Disabled";
                    }
                    catch (Exception ex)
                    {
                        log.warn("S3BucketCollector could not get versioning for bucket {}: {}", bucketName, ex.getMessage());
                    }

                    // Resolve encryption
                    List<String> encryptionAlgorithms = new ArrayList<>();
                    try
                    {
                        List<ServerSideEncryptionRule> rules = this.s3Client.getBucketEncryption(
                            GetBucketEncryptionRequest.builder().bucket(bucketName).build())
                            .serverSideEncryptionConfiguration().rules();
                        if (rules != null)
                        {
                            for (ServerSideEncryptionRule rule : rules)
                            {
                                if (rule.applyServerSideEncryptionByDefault() != null)
                                {
                                    encryptionAlgorithms.add(
                                        rule.applyServerSideEncryptionByDefault().sseAlgorithmAsString());
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        log.warn("S3BucketCollector could not get encryption for bucket {}: {}", bucketName, ex.getMessage());
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", arn);
                    attributes.put("region", region);
                    attributes.put("creationDate", bucket.creationDate() != null
                        ? bucket.creationDate().toString() : null);
                    attributes.put("versioningStatus", versioning);
                    attributes.put("encryptionAlgorithms", encryptionAlgorithms);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(bucket)
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
        catch (S3Exception e)
        {
            log.error("S3BucketCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 buckets", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 buckets", e);
        }
    }
}
