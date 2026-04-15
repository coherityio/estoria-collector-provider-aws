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
import software.amazon.awssdk.services.iam.model.ListUsersRequest;
import software.amazon.awssdk.services.iam.model.ListUsersResponse;
import software.amazon.awssdk.services.iam.model.User;

/**
 * Collects IAM users via the IAM ListUsers API.
 */
@Slf4j
public class IamUserCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamUser";

    private IamClient iamClient;

    public IamUserCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "identity", "aws")).build());
        log.debug("IamUserCollector created");
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
        log.debug("IamUserCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            ListUsersRequest.Builder requestBuilder = ListUsersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamUserCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListUsersResponse response = this.iamClient.listUsers(requestBuilder.build());
            List<User> users = response.users();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamUserCollector received {} users, nextMarker={}", users.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (User user : users)
            {
                if (user == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("userId", user.userId());
                attributes.put("userName", user.userName());
                attributes.put("arn", user.arn());
                attributes.put("path", user.path());
                attributes.put("createDate", user.createDate() != null ? user.createDate().toString() : null);
                attributes.put("passwordLastUsed", user.passwordLastUsed() != null ? user.passwordLastUsed().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(user.arn())
                        .qualifiedResourceName(user.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(user.userName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(user)
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
            log.error("IamUserCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM users", e);
        }
        catch (Exception e)
        {
            log.error("IamUserCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM users", e);
        }
    }
}
