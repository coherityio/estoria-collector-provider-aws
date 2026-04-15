package io.coherity.estoria.collector.provider.aws.security;

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
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListServerCertificatesRequest;
import software.amazon.awssdk.services.iam.model.ListServerCertificatesResponse;
import software.amazon.awssdk.services.iam.model.ServerCertificateMetadata;

/**
 * Collects IAM server certificates (legacy SSL/TLS certs) via IAM ListServerCertificates API.
 */
@Slf4j
public class IamServerCertificateCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamServerCertificate";

    private IamClient iamClient;

    public IamServerCertificateCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "certificate", "aws")).build());
        log.debug("IamServerCertificateCollector created");
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
        log.debug("IamServerCertificateCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            ListServerCertificatesRequest.Builder requestBuilder = ListServerCertificatesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamServerCertificateCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListServerCertificatesResponse response = this.iamClient.listServerCertificates(requestBuilder.build());
            List<ServerCertificateMetadata> certs = response.serverCertificateMetadataList();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamServerCertificateCollector received {} certs, nextMarker={}", certs.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (ServerCertificateMetadata cert : certs)
            {
                if (cert == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("serverCertificateId", cert.serverCertificateId());
                attributes.put("serverCertificateName", cert.serverCertificateName());
                attributes.put("arn", cert.arn());
                attributes.put("path", cert.path());
                attributes.put("uploadDate", cert.uploadDate() != null ? cert.uploadDate().toString() : null);
                attributes.put("expiration", cert.expiration() != null ? cert.expiration().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(cert.arn())
                        .qualifiedResourceName(cert.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(cert.serverCertificateName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(cert)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
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
        catch (IamException e)
        {
            log.error("IamServerCertificateCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM server certificates", e);
        }
        catch (Exception e)
        {
            log.error("IamServerCertificateCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM server certificates", e);
        }
    }
}
