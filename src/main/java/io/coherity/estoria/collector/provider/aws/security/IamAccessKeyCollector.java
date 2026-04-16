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
import software.amazon.awssdk.services.iam.model.AccessKeyMetadata;
import software.amazon.awssdk.services.iam.model.GetAccessKeyLastUsedRequest;
import software.amazon.awssdk.services.iam.model.GetAccessKeyLastUsedResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListAccessKeysRequest;
import software.amazon.awssdk.services.iam.model.ListAccessKeysResponse;
import software.amazon.awssdk.services.iam.model.ListUsersRequest;
import software.amazon.awssdk.services.iam.model.User;

/**
 * Collects IAM access key metadata (status, last used) for all users via IAM ListAccessKeys API.
 * Does NOT collect the actual key material.
 */
@Slf4j
public class IamAccessKeyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamAccessKey";


    public IamAccessKeyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("IamUser"), Set.of("security", "iam", "access-key", "aws")).build());
        log.debug("IamAccessKeyCollector created");
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
        log.debug("IamAccessKeyCollector.collect called — full scan across all users");

        IamClient iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);

        try
        {
            // Full scan: enumerate all users first, then list access keys per user
            List<User> allUsers = new ArrayList<>();
            String userMarker = null;
            do
            {
                ListUsersRequest.Builder req = ListUsersRequest.builder().maxItems(100);
                if (userMarker != null) req.marker(userMarker);
                var userResponse = iamClient.listUsers(req.build());
                allUsers.addAll(userResponse.users());
                userMarker = userResponse.isTruncated() ? userResponse.marker() : null;
            } while (userMarker != null);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (User user : allUsers)
            {
                String userName = user.userName();
                String keyMarker = null;
                do
                {
                    ListAccessKeysRequest.Builder req = ListAccessKeysRequest.builder()
                        .userName(userName).maxItems(100);
                    if (keyMarker != null) req.marker(keyMarker);
                    ListAccessKeysResponse keyResponse = iamClient.listAccessKeys(req.build());

                    for (AccessKeyMetadata key : keyResponse.accessKeyMetadata())
                    {
                        String keyId = key.accessKeyId();
                        String lastUsedRegion = null;
                        String lastUsedService = null;
                        String lastUsedDate = null;
                        try
                        {
                            GetAccessKeyLastUsedResponse lastUsed = iamClient.getAccessKeyLastUsed(
                                GetAccessKeyLastUsedRequest.builder().accessKeyId(keyId).build());
                            if (lastUsed.accessKeyLastUsed() != null)
                            {
                                lastUsedRegion  = lastUsed.accessKeyLastUsed().region();
                                lastUsedService = lastUsed.accessKeyLastUsed().serviceName();
                                lastUsedDate    = lastUsed.accessKeyLastUsed().lastUsedDate() != null
                                    ? lastUsed.accessKeyLastUsed().lastUsedDate().toString() : null;
                            }
                        }
                        catch (Exception ex)
                        {
                            log.warn("IamAccessKeyCollector could not get last used for key {}: {}", keyId, ex.getMessage());
                        }

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("accessKeyId", keyId);
                        attributes.put("userName", userName);
                        attributes.put("status", key.statusAsString());
                        attributes.put("createDate", key.createDate() != null ? key.createDate().toString() : null);
                        attributes.put("lastUsedDate", lastUsedDate);
                        attributes.put("lastUsedRegion", lastUsedRegion);
                        attributes.put("lastUsedService", lastUsedService);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(keyId)
                                .qualifiedResourceName(userName + "/" + keyId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(keyId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(key)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }

                    keyMarker = keyResponse.isTruncated() ? keyResponse.marker() : null;
                } while (keyMarker != null);
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
            log.error("IamAccessKeyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM access keys", e);
        }
        catch (Exception e)
        {
            log.error("IamAccessKeyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM access keys", e);
        }
    }
}
