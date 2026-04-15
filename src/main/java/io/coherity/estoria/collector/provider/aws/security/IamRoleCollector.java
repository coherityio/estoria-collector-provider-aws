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
import software.amazon.awssdk.services.iam.model.ListRolesRequest;
import software.amazon.awssdk.services.iam.model.ListRolesResponse;
import software.amazon.awssdk.services.iam.model.Role;

/**
 * Collects IAM roles via the IAM ListRoles API.
 */
@Slf4j
public class IamRoleCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamRole";

    private IamClient iamClient;

    public IamRoleCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "identity", "aws")).build());
        log.debug("IamRoleCollector created");
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
        log.debug("IamRoleCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            ListRolesRequest.Builder requestBuilder = ListRolesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamRoleCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListRolesResponse response = this.iamClient.listRoles(requestBuilder.build());
            List<Role> roles = response.roles();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamRoleCollector received {} roles, nextMarker={}", roles.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Role role : roles)
            {
                if (role == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("roleId", role.roleId());
                attributes.put("roleName", role.roleName());
                attributes.put("arn", role.arn());
                attributes.put("path", role.path());
                attributes.put("description", role.description());
                attributes.put("maxSessionDuration", role.maxSessionDuration());
                attributes.put("createDate", role.createDate() != null ? role.createDate().toString() : null);
                attributes.put("assumeRolePolicyDocument", role.assumeRolePolicyDocument());

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(role.arn())
                        .qualifiedResourceName(role.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(role.roleName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(role)
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
            log.error("IamRoleCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM roles", e);
        }
        catch (Exception e)
        {
            log.error("IamRoleCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM roles", e);
        }
    }
}
