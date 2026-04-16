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
import software.amazon.awssdk.services.account.model.AlternateContact;
import software.amazon.awssdk.services.account.model.AlternateContactType;
import software.amazon.awssdk.services.account.model.GetAlternateContactRequest;
import software.amazon.awssdk.services.account.model.GetAlternateContactResponse;
import software.amazon.awssdk.services.account.model.ResourceNotFoundException;

/**
 * Collects AWS Account alternate contacts (BILLING, OPERATIONS, SECURITY)
 * via the Account GetAlternateContact API.
 */
@Slf4j
public class AccountAlternateContactCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AccountAlternateContact";


    public AccountAlternateContactCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("account", "contact", "aws")).build());
        log.debug("AccountAlternateContactCollector created");
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
        log.debug("AccountAlternateContactCollector.collectEntities called");

        AccountClient accountClient = AwsClientFactory.getInstance().getAccountClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            // There are exactly 3 alternate contact types — fetch each individually
            for (AlternateContactType contactType : AlternateContactType.knownValues())
            {
                try
                {
                    GetAlternateContactRequest request = GetAlternateContactRequest.builder()
                        .alternateContactType(contactType)
                        .build();

                    GetAlternateContactResponse response = accountClient.getAlternateContact(request);
                    AlternateContact contact = response.alternateContact();

                    if (contact == null) continue;

                    String syntheticId = "arn:aws:account::" + accountId + ":alternate-contact/" + contactType.toString().toLowerCase();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("alternateContactType", contactType.toString());
                    attributes.put("name", contact.name());
                    attributes.put("emailAddress", contact.emailAddress());
                    attributes.put("phoneNumber", contact.phoneNumber());
                    attributes.put("title", contact.title());
                    attributes.put("accountId", accountId);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(syntheticId)
                            .qualifiedResourceName(syntheticId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(contactType.toString() + " contact")
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(contact)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
                catch (ResourceNotFoundException e)
                {
                    // This contact type is not configured — skip silently
                    log.debug("AccountAlternateContactCollector: {} contact not configured", contactType);
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
        catch (AccountException e)
        {
            log.error("AccountAlternateContactCollector Account error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect account alternate contacts", e);
        }
        catch (Exception e)
        {
            log.error("AccountAlternateContactCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting account alternate contacts", e);
        }
    }
}
