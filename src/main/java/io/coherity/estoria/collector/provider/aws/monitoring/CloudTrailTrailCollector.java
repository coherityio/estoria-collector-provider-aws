package io.coherity.estoria.collector.provider.aws.monitoring;

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
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudtrail.CloudTrailClient;
import software.amazon.awssdk.services.cloudtrail.model.DescribeTrailsRequest;
import software.amazon.awssdk.services.cloudtrail.model.DescribeTrailsResponse;
import software.amazon.awssdk.services.cloudtrail.model.Trail;

@Slf4j
public class CloudTrailTrailCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudTrailTrail";


    public CloudTrailTrailCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "cloudtrail", "trail", "audit", "aws")).build());
    }

    @Override public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }
    @Override public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }
    @Override public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        CloudTrailClient cloudTrailClient = AwsClientFactory.getInstance().getCloudTrailClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeTrailsResponse response = cloudTrailClient.describeTrails(
                DescribeTrailsRequest.builder().includeShadowTrails(true).build());

            List<Trail> trails = response.trailList();
            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (trails != null)
            {
                for (Trail trail : trails)
                {
                    if (trail == null)
                    {
                        continue;
                    }

                    String trailName = trail.name();
                    String qualifiedName = trail.trailARN() != null
                        ? trail.trailARN()
                        : ARNHelper.cloudTrailTrailArn(region, accountId, trailName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", trailName);
                    attributes.put("trailArn", trail.trailARN());
                    attributes.put("homeRegion", trail.homeRegion());
                    attributes.put("s3BucketName", trail.s3BucketName());
                    attributes.put("s3KeyPrefix", trail.s3KeyPrefix());
                    attributes.put("snsTopicName", trail.snsTopicName());
                    attributes.put("snsTopicArn", trail.snsTopicARN());
                    attributes.put("includeGlobalServiceEvents", trail.includeGlobalServiceEvents());
                    attributes.put("isMultiRegionTrail", trail.isMultiRegionTrail());
                    attributes.put("kmsKeyId", trail.kmsKeyId());
                    attributes.put("cloudWatchLogsLogGroupArn", trail.cloudWatchLogsLogGroupArn());
                    attributes.put("cloudWatchLogsRoleArn", trail.cloudWatchLogsRoleArn());
                    attributes.put("hasCustomEventSelectors", trail.hasCustomEventSelectors());
                    attributes.put("hasInsightSelectors", trail.hasInsightSelectors());
                    attributes.put("isOrganizationTrail", trail.isOrganizationTrail());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(trailName)
                            .qualifiedResourceName(qualifiedName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(trailName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(trail)
                        .collectedAt(now)
                        .build());
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
        catch (Exception e)
        {
            log.error("CloudTrailTrailCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudTrail trails", e);
        }
    }
}