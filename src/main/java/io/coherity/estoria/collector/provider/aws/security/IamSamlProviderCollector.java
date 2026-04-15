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
import software.amazon.awssdk.services.iam.model.ListSamlProvidersRequest;
import software.amazon.awssdk.services.iam.model.ListSamlProvidersResponse;
import software.amazon.awssdk.services.iam.model.SAMLProviderListEntry;

/**
 * Collects IAM SAML providers via the IAM ListSAMLProviders API.
 */
@Slf4j
public class IamSamlProviderCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamSamlProvider";

    private IamClient iamClient;

    public IamSamlProviderCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "saml", "federation", "aws")).build());
        log.debug("IamSamlProviderCollector created");
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
        log.debug("IamSamlProviderCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            // ListSAMLProviders returns all providers at once — no pagination
            ListSamlProvidersResponse response = this.iamClient.listSAMLProviders(
                ListSamlProvidersRequest.builder().build());
            List<SAMLProviderListEntry> providers = response.samlProviderList();

            log.debug("IamSamlProviderCollector received {} SAML providers", providers.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (SAMLProviderListEntry provider : providers)
            {
                if (provider == null) continue;

                String arn = provider.arn();
                String name = arn.contains("/") ? arn.substring(arn.lastIndexOf('/') + 1) : arn;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("arn", arn);
                attributes.put("createDate", provider.createDate() != null ? provider.createDate().toString() : null);
                attributes.put("validUntil", provider.validUntil() != null ? provider.validUntil().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(arn)
                        .qualifiedResourceName(arn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(name)
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(provider)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
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
        catch (IamException e)
        {
            log.error("IamSamlProviderCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM SAML providers", e);
        }
        catch (Exception e)
        {
            log.error("IamSamlProviderCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM SAML providers", e);
        }
    }
}
