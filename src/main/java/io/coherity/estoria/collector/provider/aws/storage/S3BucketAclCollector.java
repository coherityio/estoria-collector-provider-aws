package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Collects S3 bucket ACLs for all buckets.
 */
@Slf4j
public class S3BucketAclCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "S3BucketAcl";

    private S3Client s3Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("S3Bucket"))
            .tags(Set.of("storage", "s3", "acl", "aws"))
            .build();

    public S3BucketAclCollector()
    {
        log.debug("S3BucketAclCollector created");
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
        log.debug("S3BucketAclCollector.collect called");

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

                    GetBucketAclResponse aclResponse;
                    try
                    {
                        aclResponse = this.s3Client.getBucketAcl(
                            GetBucketAclRequest.builder().bucket(bucketName).build());
                    }
                    catch (S3Exception ex)
                    {
                        log.warn("S3BucketAclCollector error fetching ACL for {}: {}", bucketName, ex.getMessage());
                        continue;
                    }

                    String bucketArn = ARNHelper.s3BucketArn(bucketName);
                    String id = bucketArn + "/acl";

                    List<Map<String, String>> grants = new ArrayList<>();
                    if (aclResponse.grants() != null)
                    {
                        for (Grant grant : aclResponse.grants())
                        {
                            Map<String, String> g = new HashMap<>();
                            g.put("permission", grant.permissionAsString());
                            if (grant.grantee() != null)
                            {
                                g.put("granteeType", grant.grantee().typeAsString());
                                g.put("granteeId", grant.grantee().id());
                                g.put("granteeUri", grant.grantee().uri());
                                g.put("granteeDisplayName", grant.grantee().displayName());
                            }
                            grants.add(g);
                        }
                    }

                    Map<String, Object> ownerMap = new HashMap<>();
                    if (aclResponse.owner() != null)
                    {
                        ownerMap.put("id", aclResponse.owner().id());
                        ownerMap.put("displayName", aclResponse.owner().displayName());
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("bucketName", bucketName);
                    attributes.put("bucketArn", bucketArn);
                    attributes.put("owner", ownerMap);
                    attributes.put("grants", grants);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(bucketName + "/acl")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(aclResponse)
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
            log.error("S3BucketAclCollector S3 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 bucket ACLs", e);
        }
        catch (Exception e)
        {
            log.error("S3BucketAclCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 bucket ACLs", e);
        }
    }
}
