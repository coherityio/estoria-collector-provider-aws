package io.coherity.estoria.collector.provider.aws.storage;

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
import software.amazon.awssdk.services.efs.EfsClient;
import software.amazon.awssdk.services.efs.model.AccessPointDescription;
import software.amazon.awssdk.services.efs.model.DescribeAccessPointsRequest;
import software.amazon.awssdk.services.efs.model.DescribeAccessPointsResponse;
import software.amazon.awssdk.services.efs.model.EfsException;
import software.amazon.awssdk.services.efs.model.Tag;

/**
 * Collects EFS access points via the EFS DescribeAccessPoints API.
 */
@Slf4j
public class EfsAccessPointCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EfsAccessPoint";

    private EfsClient efsClient;

    public EfsAccessPointCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("EfsFileSystem"), Set.of("storage", "efs", "access-point", "aws")).build());
        log.debug("EfsAccessPointCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("EfsAccessPointCollector.collect called");

        if (this.efsClient == null)
        {
            this.efsClient = AwsClientFactory.getInstance().getEfsClient(providerContext);
        }

        try
        {
            DescribeAccessPointsRequest.Builder requestBuilder = DescribeAccessPointsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EfsAccessPointCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeAccessPointsResponse response = this.efsClient.describeAccessPoints(requestBuilder.build());
            List<AccessPointDescription> accessPoints = response.accessPoints();
            String nextToken = response.nextToken();

            log.debug("EfsAccessPointCollector received {} access points, nextToken={}",
                accessPoints != null ? accessPoints.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (accessPoints != null)
            {
                for (AccessPointDescription ap : accessPoints)
                {
                    if (ap == null) continue;

                    String apId  = ap.accessPointId();
                    String apArn = ap.accessPointArn();

                    Map<String, String> tags = ap.tags() == null ? new HashMap<>()
                        : ap.tags().stream().collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", apId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("accessPointId", apId);
                    attributes.put("accessPointArn", apArn);
                    attributes.put("fileSystemId", ap.fileSystemId());
                    attributes.put("clientToken", ap.clientToken());
                    attributes.put("lifeCycleState", ap.lifeCycleStateAsString());
                    attributes.put("ownerId", ap.ownerId());
                    if (ap.posixUser() != null)
                    {
                        attributes.put("posixUid", ap.posixUser().uid());
                        attributes.put("posixGid", ap.posixUser().gid());
                    }
                    if (ap.rootDirectory() != null)
                    {
                        attributes.put("rootDirectoryPath", ap.rootDirectory().path());
                    }
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(apArn != null ? apArn : apId)
                            .qualifiedResourceName(apArn != null ? apArn : apId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(ap)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (EfsException e)
        {
            log.error("EfsAccessPointCollector EFS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EFS access points", e);
        }
        catch (Exception e)
        {
            log.error("EfsAccessPointCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EFS access points", e);
        }
    }
}
