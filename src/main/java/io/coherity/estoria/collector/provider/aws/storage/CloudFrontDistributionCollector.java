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
import software.amazon.awssdk.services.cloudfront.model.DistributionSummary;
import software.amazon.awssdk.services.cloudfront.model.ListDistributionsRequest;
import software.amazon.awssdk.services.cloudfront.model.ListDistributionsResponse;

/**
 * Collects CloudFront distributions via the CloudFront ListDistributions API.
 */
@Slf4j
public class CloudFrontDistributionCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudFrontDistribution";

    private CloudFrontClient cloudFrontClient;

    public CloudFrontDistributionCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "cdn", "cloudfront", "aws")).build());
        log.debug("CloudFrontDistributionCollector created");
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
        log.debug("CloudFrontDistributionCollector.collect called");

        if (this.cloudFrontClient == null)
        {
            this.cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);
        }

        try
        {
            ListDistributionsRequest.Builder requestBuilder = ListDistributionsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontDistributionCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListDistributionsResponse response = this.cloudFrontClient.listDistributions(requestBuilder.build());
            List<DistributionSummary> distributions = response.distributionList() != null
                ? response.distributionList().items() : null;
            String nextMarker = response.distributionList() != null
                ? response.distributionList().nextMarker() : null;

            log.debug("CloudFrontDistributionCollector received {} distributions, nextMarker={}",
                distributions != null ? distributions.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (distributions != null)
            {
                for (DistributionSummary dist : distributions)
                {
                    if (dist == null) continue;

                    String id  = dist.id();
                    String arn = dist.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("distributionId", id);
                    attributes.put("distributionArn", arn);
                    attributes.put("domainName", dist.domainName());
                    attributes.put("status", dist.status());
                    attributes.put("enabled", dist.enabled());
                    attributes.put("comment", dist.comment());
                    attributes.put("priceClass", dist.priceClassAsString());
                    attributes.put("httpVersion", dist.httpVersionAsString());
                    attributes.put("isIPV6Enabled", dist.isIPV6Enabled());
                    attributes.put("webAclId", dist.webACLId());
                    attributes.put("lastModifiedTime", dist.lastModifiedTime() != null
                        ? dist.lastModifiedTime().toString() : null);
                    if (dist.aliases() != null)
                    {
                        attributes.put("aliases", dist.aliases().items());
                    }
                    if (dist.origins() != null && dist.origins().items() != null)
                    {
                        List<String> originDomains = new ArrayList<>();
                        dist.origins().items().forEach(o -> originDomains.add(o.domainName()));
                        attributes.put("originDomains", originDomains);
                    }
                    if (dist.viewerCertificate() != null)
                    {
                        attributes.put("certificateSource",
                            dist.viewerCertificate().certificateSourceAsString());
                        attributes.put("sslSupportMethod",
                            dist.viewerCertificate().sslSupportMethodAsString());
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : id)
                            .qualifiedResourceName(arn != null ? arn : id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(id)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(dist)
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
            log.error("CloudFrontDistributionCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront distributions", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontDistributionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront distributions", e);
        }
    }
}
