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
import software.amazon.awssdk.services.iam.model.GetOpenIdConnectProviderRequest;
import software.amazon.awssdk.services.iam.model.GetOpenIdConnectProviderResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListOpenIdConnectProvidersRequest;
import software.amazon.awssdk.services.iam.model.ListOpenIdConnectProvidersResponse;
import software.amazon.awssdk.services.iam.model.OpenIDConnectProviderListEntry;

/**
 * Collects IAM OIDC providers via the IAM ListOpenIDConnectProviders API.
 */
@Slf4j
public class IamOpenIdConnectProviderCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "IamOpenIdConnectProvider";

    private IamClient iamClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "iam", "oidc", "federation", "aws"))
            .build();

    public IamOpenIdConnectProviderCollector()
    {
        log.debug("IamOpenIdConnectProviderCollector created");
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
        log.debug("IamOpenIdConnectProviderCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            // ListOpenIDConnectProviders returns all at once — no pagination
            ListOpenIdConnectProvidersResponse listResponse = this.iamClient.listOpenIDConnectProviders(
                ListOpenIdConnectProvidersRequest.builder().build());
            List<OpenIDConnectProviderListEntry> entries = listResponse.openIDConnectProviderList();

            log.debug("IamOpenIdConnectProviderCollector received {} OIDC providers", entries.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (OpenIDConnectProviderListEntry entry : entries)
            {
                if (entry == null) continue;

                String arn = entry.arn();
                String name = arn.contains("/") ? arn.substring(arn.lastIndexOf('/') + 1) : arn;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("arn", arn);

                // Fetch details
                try
                {
                    GetOpenIdConnectProviderResponse detail = this.iamClient.getOpenIDConnectProvider(
                        GetOpenIdConnectProviderRequest.builder().openIDConnectProviderArn(arn).build());
                    attributes.put("url", detail.url());
                    attributes.put("clientIdList", detail.clientIDList());
                    attributes.put("thumbprintList", detail.thumbprintList());
                    attributes.put("createDate", detail.createDate() != null ? detail.createDate().toString() : null);
                }
                catch (Exception ex)
                {
                    log.warn("IamOpenIdConnectProviderCollector could not get details for {}: {}", arn, ex.getMessage());
                }

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(arn)
                        .qualifiedResourceName(arn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(name)
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(entry)
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
            log.error("IamOpenIdConnectProviderCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM OIDC providers", e);
        }
        catch (Exception e)
        {
            log.error("IamOpenIdConnectProviderCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM OIDC providers", e);
        }
    }
}
