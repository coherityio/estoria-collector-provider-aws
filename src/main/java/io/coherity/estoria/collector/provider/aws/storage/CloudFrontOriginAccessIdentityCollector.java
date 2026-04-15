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
import software.amazon.awssdk.services.cloudfront.model.CloudFrontOriginAccessIdentitySummary;
import software.amazon.awssdk.services.cloudfront.model.ListCloudFrontOriginAccessIdentitiesRequest;
import software.amazon.awssdk.services.cloudfront.model.ListCloudFrontOriginAccessIdentitiesResponse;

/**
 * Collects CloudFront Origin Access Identities (OAIs) via the CloudFront ListCloudFrontOriginAccessIdentities API.
 */
@Slf4j
public class CloudFrontOriginAccessIdentityCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudFrontOriginAccessIdentity";

    private CloudFrontClient cloudFrontClient;

    public CloudFrontOriginAccessIdentityCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "cdn", "cloudfront", "oai", "aws")).build());
        log.debug("CloudFrontOriginAccessIdentityCollector created");
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
        log.debug("CloudFrontOriginAccessIdentityCollector.collect called");

        if (this.cloudFrontClient == null)
        {
            this.cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);
        }

        try
        {
            ListCloudFrontOriginAccessIdentitiesRequest.Builder requestBuilder =
                ListCloudFrontOriginAccessIdentitiesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontOriginAccessIdentityCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListCloudFrontOriginAccessIdentitiesResponse response =
                this.cloudFrontClient.listCloudFrontOriginAccessIdentities(requestBuilder.build());
            List<CloudFrontOriginAccessIdentitySummary> oais = response.cloudFrontOriginAccessIdentityList() != null
                ? response.cloudFrontOriginAccessIdentityList().items() : null;
            String nextMarker = response.cloudFrontOriginAccessIdentityList() != null
                ? response.cloudFrontOriginAccessIdentityList().nextMarker() : null;

            log.debug("CloudFrontOriginAccessIdentityCollector received {} OAIs, nextMarker={}",
                oais != null ? oais.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (oais != null)
            {
                for (CloudFrontOriginAccessIdentitySummary oai : oais)
                {
                    if (oai == null) continue;

                    String id = oai.id();
                    String s3CanonicalUserId = oai.s3CanonicalUserId();
                    String comment = oai.comment();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", id);
                    attributes.put("s3CanonicalUserId", s3CanonicalUserId);
                    attributes.put("comment", comment);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(id)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(comment != null && !comment.isBlank() ? comment : id)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(oai)
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
            log.error("CloudFrontOriginAccessIdentityCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront Origin Access Identities", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontOriginAccessIdentityCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront Origin Access Identities", e);
        }
    }
}
