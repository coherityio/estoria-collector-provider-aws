package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
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
import software.amazon.awssdk.services.iam.model.Group;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListGroupsRequest;
import software.amazon.awssdk.services.iam.model.ListGroupsResponse;

/**
 * Collects IAM groups via the IAM ListGroups API.
 */
@Slf4j
public class IamGroupCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "IamGroup";

    private IamClient iamClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "iam", "identity", "aws"))
            .build();

    public IamGroupCollector()
    {
        log.debug("IamGroupCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("IamGroupCollector.collect called");

        if (this.iamClient == null)
        {
            this.iamClient = AwsClientFactory.getInstance().getIamClient(providerContext);
        }

        try
        {
            ListGroupsRequest.Builder requestBuilder = ListGroupsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("IamGroupCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListGroupsResponse response = this.iamClient.listGroups(requestBuilder.build());
            List<Group> groups = response.groups();
            String nextMarker = response.isTruncated() ? response.marker() : null;

            log.debug("IamGroupCollector received {} groups, nextMarker={}", groups.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Group group : groups)
            {
                if (group == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("groupId", group.groupId());
                attributes.put("groupName", group.groupName());
                attributes.put("arn", group.arn());
                attributes.put("path", group.path());
                attributes.put("createDate", group.createDate() != null ? group.createDate().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(group.arn())
                        .qualifiedResourceName(group.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(group.groupName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(group)
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
            log.error("IamGroupCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect IAM groups", e);
        }
        catch (Exception e)
        {
            log.error("IamGroupCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting IAM groups", e);
        }
    }
}
