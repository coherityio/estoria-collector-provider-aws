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
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteRequest;
import software.amazon.awssdk.services.s3.model.GetBucketWebsiteResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Collects S3 bucket static website hosting configuration.
 */
@Slf4j
public class S3BucketWebsiteCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "S3BucketWebsite";


    public S3BucketWebsiteCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("S3Bucket"), Set.of("storage", "s3", "website", "aws")).build());
        log.debug("S3BucketWebsiteCollector created");
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
        log.debug("S3BucketWebsiteCollector.collect called");

        S3Client s3Client = AwsClientFactory.getInstance().getS3Client(providerContext);

        try
        {
            ListBucketsResponse listResponse = s3Client.listBuckets();
            List<Bucket> buckets = listResponse.buckets();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (buckets != null)
            {
                for (Bucket bucket : buckets)
                {
                    if (bucket == null) continue;

                    String bucketName = bucket.name();

                    GetBucketWebsiteResponse websiteResponse;
                    try
                    {
                        websiteResponse = s3Client.getBucketWebsite(
                            GetBucketWebsiteRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        String code = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : "";
                        if ("NoSuchWebsiteConfiguration".equals(code))
                        {
                            log.debug("S3BucketWebsiteCollector no website config for bucket: {}", bucketName);
                            continue;
                        }
                        log.warn("S3BucketWebsiteCollector error for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/website";

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("indexDocument",
                        websiteResponse.indexDocument() != null ? websiteResponse.indexDocument().suffix() : null);
                    attributes.put("errorDocument",
                        websiteResponse.errorDocument() != null ? websiteResponse.errorDocument().key() : null);
                    if (websiteResponse.redirectAllRequestsTo() != null)
                    {
                        attributes.put("redirectHostName", websiteResponse.redirectAllRequestsTo().hostName());
                        attributes.put("redirectProtocol",
                            websiteResponse.redirectAllRequestsTo().protocolAsString());
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/website")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(websiteResponse)
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
            log.error("S3BucketWebsiteCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 bucket website configs", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketWebsiteCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 bucket website configs", e);
        }
    }
}
