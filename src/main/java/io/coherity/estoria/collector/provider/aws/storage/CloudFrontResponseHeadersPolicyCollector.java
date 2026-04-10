package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
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
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudfront.model.CloudFrontException;
import software.amazon.awssdk.services.cloudfront.model.ListResponseHeadersPoliciesRequest;
import software.amazon.awssdk.services.cloudfront.model.ListResponseHeadersPoliciesResponse;
import software.amazon.awssdk.services.cloudfront.model.ResponseHeadersPolicySummary;

/**
 * Collects CloudFront Response Headers Policies via the CloudFront ListResponseHeadersPolicies API.
 */
@Slf4j
public class CloudFrontResponseHeadersPolicyCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "CloudFrontResponseHeadersPolicy";

    private CloudFrontClient cloudFrontClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("storage", "cdn", "cloudfront", "response-headers-policy", "aws"))
            .build();

    public CloudFrontResponseHeadersPolicyCollector()
    {
        log.debug("CloudFrontResponseHeadersPolicyCollector created");
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
        log.debug("CloudFrontResponseHeadersPolicyCollector.collect called");

        if (this.cloudFrontClient == null)
        {
            this.cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);
        }

        try
        {
            ListResponseHeadersPoliciesRequest.Builder requestBuilder =
                ListResponseHeadersPoliciesRequest.builder().type("custom");

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontResponseHeadersPolicyCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListResponseHeadersPoliciesResponse response =
                this.cloudFrontClient.listResponseHeadersPolicies(requestBuilder.build());
            List<ResponseHeadersPolicySummary> policies = response.responseHeadersPolicyList() != null
                ? response.responseHeadersPolicyList().items() : null;
            String nextMarker = response.responseHeadersPolicyList() != null
                ? response.responseHeadersPolicyList().nextMarker() : null;

            log.debug("CloudFrontResponseHeadersPolicyCollector received {} policies, nextMarker={}",
                policies != null ? policies.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (policies != null)
            {
                for (ResponseHeadersPolicySummary summary : policies)
                {
                    if (summary == null || summary.responseHeadersPolicy() == null) continue;

                    String id = summary.responseHeadersPolicy().id();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", id);
                    attributes.put("type", summary.typeAsString());
                    attributes.put("lastModifiedTime", summary.responseHeadersPolicy().lastModifiedTime() != null
                        ? summary.responseHeadersPolicy().lastModifiedTime().toString() : null);

                    String name = id;
                    if (summary.responseHeadersPolicy().responseHeadersPolicyConfig() != null)
                    {
                        var config = summary.responseHeadersPolicy().responseHeadersPolicyConfig();
                        name = config.name();
                        attributes.put("name", config.name());
                        attributes.put("comment", config.comment());
                        if (config.securityHeadersConfig() != null && config.securityHeadersConfig().contentSecurityPolicy() != null)
                        {
                            attributes.put("contentSecurityPolicy",
                                config.securityHeadersConfig().contentSecurityPolicy().contentSecurityPolicy());
                        }
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
            log.error("CloudFrontResponseHeadersPolicyCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront Response Headers Policies", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontResponseHeadersPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront Response Headers Policies", e);
        }
    }
}
