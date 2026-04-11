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
import software.amazon.awssdk.services.s3.model.GetBucketReplicationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketReplicationResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ReplicationRule;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Collects S3 bucket cross-region replication rules.
 */
@Slf4j
public class S3BucketReplicationCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "S3BucketReplication";

    private S3Client s3Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("S3Bucket"))
            .tags(Set.of("storage", "s3", "replication", "aws"))
            .build();

    public S3BucketReplicationCollector()
    {
        log.debug("S3BucketReplicationCollector created");
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
        log.debug("S3BucketReplicationCollector.collect called");

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

                    GetBucketReplicationResponse repResponse;
                    try
                    {
                        repResponse = this.s3Client.getBucketReplication(
                            GetBucketReplicationRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        String code = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : "";
                        if ("ReplicationConfigurationNotFoundError".equals(code))
                        {
                            log.debug("S3BucketReplicationCollector no replication config for bucket: {}", bucketName);
                            continue;
                        }
                        log.warn("S3BucketReplicationCollector error for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    if (repResponse.replicationConfiguration() == null) continue;

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/replication";

                    String role = repResponse.replicationConfiguration().role();
                    List<Map<String, Object>> rules = new ArrayList<>();
                    if (repResponse.replicationConfiguration().rules() != null)
                    {
                        for (ReplicationRule rule : repResponse.replicationConfiguration().rules())
                        {
                            Map<String, Object> r = new HashMap<>();
                            r.put("id", rule.id());
                            r.put("status", rule.statusAsString());
                            r.put("priority", rule.priority());
                            r.put("prefix", rule.prefix());
                            if (rule.destination() != null)
                            {
                                r.put("destinationBucket", rule.destination().bucket());
                                r.put("destinationStorageClass", rule.destination().storageClassAsString());
                            }
                            rules.add(r);
                        }
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("role", role);
                    attributes.put("rules", rules);
                    attributes.put("ruleCount", rules.size());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/replication")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(repResponse)
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
            log.error("S3BucketReplicationCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 bucket replication configs", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketReplicationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 bucket replication configs", e);
        }
    }
}
