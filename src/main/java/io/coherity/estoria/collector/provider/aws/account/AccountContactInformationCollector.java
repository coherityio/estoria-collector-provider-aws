package io.coherity.estoria.collector.provider.aws.account;

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
import software.amazon.awssdk.services.account.AccountClient;
import software.amazon.awssdk.services.account.model.AccountException;
import software.amazon.awssdk.services.account.model.ContactInformation;
import software.amazon.awssdk.services.account.model.GetContactInformationRequest;
import software.amazon.awssdk.services.account.model.GetContactInformationResponse;

/**
 * Collects the primary contact information for an AWS account
 * via the Account GetContactInformation API.
 */
@Slf4j
public class AccountContactInformationCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "AccountContactInformation";

    private AccountClient accountClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("account", "contact", "aws"))
            .build();

    public AccountContactInformationCollector()
    {
        log.debug("AccountContactInformationCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
        log.debug("AccountContactInformationCollector.collectEntities called");

        if (this.accountClient == null)
        {
            this.accountClient = AwsClientFactory.getInstance().getAccountClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();

            GetContactInformationResponse response = this.accountClient.getContactInformation(
                GetContactInformationRequest.builder().build());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            ContactInformation contact = response.contactInformation();
            if (contact != null)
            {
                String syntheticId = "arn:aws:account::" + accountId + ":contact-information";

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("accountId", accountId);
                attributes.put("fullName", contact.fullName());
                attributes.put("addressLine1", contact.addressLine1());
                attributes.put("addressLine2", contact.addressLine2());
                attributes.put("addressLine3", contact.addressLine3());
                attributes.put("city", contact.city());
                attributes.put("stateOrRegion", contact.stateOrRegion());
                attributes.put("postalCode", contact.postalCode());
                attributes.put("countryCode", contact.countryCode());
                attributes.put("phoneNumber", contact.phoneNumber());
                attributes.put("companyName", contact.companyName());
                attributes.put("websiteUrl", contact.websiteUrl());
                attributes.put("districtOrCounty", contact.districtOrCounty());

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(syntheticId)
                        .qualifiedResourceName(syntheticId)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name("Contact information for " + accountId)
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(contact)
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
        catch (AccountException e)
        {
            log.error("AccountContactInformationCollector Account error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect account contact information", e);
        }
        catch (Exception e)
        {
            log.error("AccountContactInformationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting account contact information", e);
        }
    }
}
