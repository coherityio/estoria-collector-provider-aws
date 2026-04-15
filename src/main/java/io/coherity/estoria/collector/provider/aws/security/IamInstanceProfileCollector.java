package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.iam.model.InstanceProfile;
import software.amazon.awssdk.services.iam.model.ListInstanceProfilesRequest;
import software.amazon.awssdk.services.iam.model.ListInstanceProfilesResponse;

/**
 * Collects IAM instance profiles via the IAM ListInstanceProfiles API.
 */
@Slf4j
public class IamInstanceProfileCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "IamInstanceProfile";

    private IamClient iamClient;

    public IamInstanceProfileCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "iam", "instance-profile", "aws")).build());
        log.debug("IamInstanceProfileCollector created");
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
        log.debug("IamInstanceProfileCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            ListInstanceProfilesRequest.Builder requestBuilder = ListInstanceProfilesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamInstanceProfileCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListInstanceProfilesResponse response = this.iamClient.listInstanceProfiles(requestBuilder.build());
            List<InstanceProfile> profiles = response.instanceProfiles();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamInstanceProfileCollector received {} profiles, nextMarker={}", profiles.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (InstanceProfile profile : profiles)
            {
                if (profile == null) continue;

                List<String> roleNames = profile.roles() == null ? List.of()
                    : profile.roles().stream().map(r -> r.roleName()).collect(Collectors.toList());

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("instanceProfileId", profile.instanceProfileId());
                attributes.put("instanceProfileName", profile.instanceProfileName());
                attributes.put("arn", profile.arn());
                attributes.put("path", profile.path());
                attributes.put("createDate", profile.createDate() != null ? profile.createDate().toString() : null);
                attributes.put("roles", roleNames);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(profile.arn())
                        .qualifiedResourceName(profile.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(profile.instanceProfileName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(profile)
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
            log.error("IamInstanceProfileCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM instance profiles", e);
        }
        catch (Exception e)
        {
            log.error("IamInstanceProfileCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM instance profiles", e);
        }
    }
}
