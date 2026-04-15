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
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketNotificationConfigurationResponse;
import software.amazon.awssdk.services.s3.model.LambdaFunctionConfiguration;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.QueueConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.TopicConfiguration;

/**
 * Collects S3 bucket event notification configurations.
 */
@Slf4j
public class S3BucketNotificationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "S3BucketNotification";

    private S3Client s3Client;

    public S3BucketNotificationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("S3Bucket"), Set.of("storage", "s3", "notification", "aws")).build());
        log.debug("S3BucketNotificationCollector created");
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
        log.debug("S3BucketNotificationCollector.collect called");

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

                    GetBucketNotificationConfigurationResponse notifResponse;
                    try
                    {
                        notifResponse = this.s3Client.getBucketNotificationConfiguration(
                            GetBucketNotificationConfigurationRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        log.warn("S3BucketNotificationCollector error for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    boolean hasNotifications =
                        (notifResponse.lambdaFunctionConfigurations() != null && !notifResponse.lambdaFunctionConfigurations().isEmpty())
                        || (notifResponse.queueConfigurations() != null && !notifResponse.queueConfigurations().isEmpty())
                        || (notifResponse.topicConfigurations() != null && !notifResponse.topicConfigurations().isEmpty());

                    if (!hasNotifications) continue;

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/notification";

                    List<Map<String, Object>> lambdaConfigs = new ArrayList<>();
                    if (notifResponse.lambdaFunctionConfigurations() != null)
                    {
                        for (LambdaFunctionConfiguration lfc : notifResponse.lambdaFunctionConfigurations())
                        {
                            Map<String, Object> m = new HashMap<>();
                            m.put("id", lfc.id());
                            m.put("lambdaFunctionArn", lfc.lambdaFunctionArn());
                            m.put("events", lfc.eventsAsStrings());
                            lambdaConfigs.add(m);
                        }
                    }

                    List<Map<String, Object>> queueConfigs = new ArrayList<>();
                    if (notifResponse.queueConfigurations() != null)
                    {
                        for (QueueConfiguration qc : notifResponse.queueConfigurations())
                        {
                            Map<String, Object> m = new HashMap<>();
                            m.put("id", qc.id());
                            m.put("queueArn", qc.queueArn());
                            m.put("events", qc.eventsAsStrings());
                            queueConfigs.add(m);
                        }
                    }

                    List<Map<String, Object>> topicConfigs = new ArrayList<>();
                    if (notifResponse.topicConfigurations() != null)
                    {
                        for (TopicConfiguration tc : notifResponse.topicConfigurations())
                        {
                            Map<String, Object> m = new HashMap<>();
                            m.put("id", tc.id());
                            m.put("topicArn", tc.topicArn());
                            m.put("events", tc.eventsAsStrings());
                            topicConfigs.add(m);
                        }
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("lambdaFunctionConfigurations", lambdaConfigs);
                    attributes.put("queueConfigurations", queueConfigs);
                    attributes.put("topicConfigurations", topicConfigs);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/notification")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(notifResponse)
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
            log.error("S3BucketNotificationCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 bucket notification configs", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketNotificationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 bucket notification configs", e);
        }
    }
}
