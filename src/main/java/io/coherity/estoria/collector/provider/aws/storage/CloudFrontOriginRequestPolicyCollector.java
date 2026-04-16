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
import software.amazon.awssdk.services.cloudfront.model.CloudFrontException;
import software.amazon.awssdk.services.cloudfront.model.ListOriginRequestPoliciesRequest;
import software.amazon.awssdk.services.cloudfront.model.ListOriginRequestPoliciesResponse;
import software.amazon.awssdk.services.cloudfront.model.OriginRequestPolicySummary;

/**
 * Collects CloudFront Origin Request Policies via the CloudFront ListOriginRequestPolicies API.
 */
@Slf4j
public class CloudFrontOriginRequestPolicyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudFrontOriginRequestPolicy";


    public CloudFrontOriginRequestPolicyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "cdn", "cloudfront", "origin-request-policy", "aws")).build());
        log.debug("CloudFrontOriginRequestPolicyCollector created");
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
        log.debug("CloudFrontOriginRequestPolicyCollector.collect called");

        CloudFrontClient cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);

        try
        {
            ListOriginRequestPoliciesRequest.Builder requestBuilder =
                ListOriginRequestPoliciesRequest.builder().type("custom");

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontOriginRequestPolicyCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListOriginRequestPoliciesResponse response =
                cloudFrontClient.listOriginRequestPolicies(requestBuilder.build());
            List<OriginRequestPolicySummary> policies = response.originRequestPolicyList() != null
                ? response.originRequestPolicyList().items() : null;
            String nextMarker = response.originRequestPolicyList() != null
                ? response.originRequestPolicyList().nextMarker() : null;

            log.debug("CloudFrontOriginRequestPolicyCollector received {} policies, nextMarker={}",
                policies != null ? policies.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (policies != null)
            {
                for (OriginRequestPolicySummary summary : policies)
                {
                    if (summary == null || summary.originRequestPolicy() == null) continue;

                    String id = summary.originRequestPolicy().id();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", id);
                    attributes.put("type", summary.typeAsString());
                    attributes.put("lastModifiedTime", summary.originRequestPolicy().lastModifiedTime() != null
                        ? summary.originRequestPolicy().lastModifiedTime().toString() : null);

                    String name = id;
                    if (summary.originRequestPolicy().originRequestPolicyConfig() != null)
                    {
                        var config = summary.originRequestPolicy().originRequestPolicyConfig();
                        name = config.name();
                        attributes.put("name", config.name());
                        attributes.put("comment", config.comment());
                    }

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
            log.error("CloudFrontOriginRequestPolicyCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront Origin Request Policies", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontOriginRequestPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront Origin Request Policies", e);
        }
    }
}
