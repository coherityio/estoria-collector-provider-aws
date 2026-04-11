package io.coherity.estoria.collector.provider.aws.storage;

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
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudfront.model.CachePolicySummary;
import software.amazon.awssdk.services.cloudfront.model.CloudFrontException;
import software.amazon.awssdk.services.cloudfront.model.ListCachePoliciesRequest;
import software.amazon.awssdk.services.cloudfront.model.ListCachePoliciesResponse;

/**
 * Collects CloudFront Cache Policies via the CloudFront ListCachePolicies API.
 */
@Slf4j
public class CloudFrontCachePolicyCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "CloudFrontCachePolicy";

    private CloudFrontClient cloudFrontClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("storage", "cdn", "cloudfront", "cache-policy", "aws"))
            .build();

    public CloudFrontCachePolicyCollector()
    {
        log.debug("CloudFrontCachePolicyCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.AWS_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("CloudFrontCachePolicyCollector.collect called");

        if (this.cloudFrontClient == null)
        {
            this.cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);
        }

        try
        {
            ListCachePoliciesRequest.Builder requestBuilder = ListCachePoliciesRequest.builder()
                .type("custom");

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontCachePolicyCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListCachePoliciesResponse response = this.cloudFrontClient.listCachePolicies(requestBuilder.build());
            List<CachePolicySummary> policies = response.cachePolicyList() != null
                ? response.cachePolicyList().items() : null;
            String nextMarker = response.cachePolicyList() != null
                ? response.cachePolicyList().nextMarker() : null;

            log.debug("CloudFrontCachePolicyCollector received {} cache policies, nextMarker={}",
                policies != null ? policies.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (policies != null)
            {
                for (CachePolicySummary summary : policies)
                {
                    if (summary == null || summary.cachePolicy() == null) continue;

                    String id = summary.cachePolicy().id();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", id);
                    attributes.put("type", summary.typeAsString());
                    attributes.put("lastModifiedTime", summary.cachePolicy().lastModifiedTime() != null
                        ? summary.cachePolicy().lastModifiedTime().toString() : null);

                    if (summary.cachePolicy().cachePolicyConfig() != null)
                    {
                        var config = summary.cachePolicy().cachePolicyConfig();
                        attributes.put("name", config.name());
                        attributes.put("comment", config.comment());
                        attributes.put("defaultTtl", config.defaultTTL());
                        attributes.put("maxTtl", config.maxTTL());
                        attributes.put("minTtl", config.minTTL());
                    }

                    String name = summary.cachePolicy().cachePolicyConfig() != null
                        ? summary.cachePolicy().cachePolicyConfig().name() : id;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(summary)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextMarker = nextMarker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextMarker).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (CloudFrontException e)
        {
            log.error("CloudFrontCachePolicyCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront Cache Policies", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontCachePolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront Cache Policies", e);
        }
    }
}
