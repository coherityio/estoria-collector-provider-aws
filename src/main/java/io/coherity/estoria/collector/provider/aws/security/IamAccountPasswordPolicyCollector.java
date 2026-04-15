package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
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
import software.amazon.awssdk.services.iam.model.GetAccountPasswordPolicyRequest;
import software.amazon.awssdk.services.iam.model.GetAccountPasswordPolicyResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.PasswordPolicy;

/**
 * Collects the IAM account password policy via IAM GetAccountPasswordPolicy API.
 * Emits a single entity per account.
 */
@Slf4j
public class IamAccountPasswordPolicyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamAccountPasswordPolicy";

    private IamClient iamClient;

    public IamAccountPasswordPolicyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "password-policy", "aws")).build());
        log.debug("IamAccountPasswordPolicyCollector created");
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
        log.debug("IamAccountPasswordPolicyCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            GetAccountPasswordPolicyResponse response = this.iamClient.getAccountPasswordPolicy(
                GetAccountPasswordPolicyRequest.builder().build());
            PasswordPolicy policy = response.passwordPolicy();

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("minimumPasswordLength", policy.minimumPasswordLength());
            attributes.put("requireSymbols", policy.requireSymbols());
            attributes.put("requireNumbers", policy.requireNumbers());
            attributes.put("requireUppercaseCharacters", policy.requireUppercaseCharacters());
            attributes.put("requireLowercaseCharacters", policy.requireLowercaseCharacters());
            attributes.put("allowUsersToChangePassword", policy.allowUsersToChangePassword());
            attributes.put("expirePasswords", policy.expirePasswords());
            attributes.put("maxPasswordAge", policy.maxPasswordAge());
            attributes.put("passwordReusePrevention", policy.passwordReusePrevention());
            attributes.put("hardExpiry", policy.hardExpiry());

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id("iam-account-password-policy")
                    .qualifiedResourceName("iam-account-password-policy")
                    .build())
                .entityType(ENTITY_TYPE)
                .name("AccountPasswordPolicy")
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(policy)
                .collectedAt(Instant.now())
                .build();

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", 1);

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return List.of(entity); }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (IamException e)
        {
            // NoSuchEntity means no password policy has been set — return empty
            if (e.awsErrorDetails() != null && "NoSuchEntity".equals(e.awsErrorDetails().errorCode()))
            {
                log.debug("IamAccountPasswordPolicyCollector: no password policy set for this account");
                Map<String, Object> metadataValues = new HashMap<>();
                metadataValues.put("count", 0);
                return new CollectorCursor()
                {
                    @Override public List<CloudEntity> getEntities() { return List.of(); }
                    @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                    @Override public CursorMetadata getMetadata()
                    {
                        return CursorMetadata.builder().values(metadataValues).build();
                    }
                };
            }
            log.error("IamAccountPasswordPolicyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM account password policy", e);
        }
        catch (Exception e)
        {
            log.error("IamAccountPasswordPolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM account password policy", e);
        }
    }
}
