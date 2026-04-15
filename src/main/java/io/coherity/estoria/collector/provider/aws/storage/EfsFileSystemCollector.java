package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
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
import software.amazon.awssdk.services.efs.model.DescribeFileSystemsRequest;
import software.amazon.awssdk.services.efs.model.DescribeFileSystemsResponse;
import software.amazon.awssdk.services.efs.model.EfsException;
import software.amazon.awssdk.services.efs.model.FileSystemDescription;
import software.amazon.awssdk.services.efs.model.Tag;

/**
 * Collects EFS file systems via the EFS DescribeFileSystems API.
 */
@Slf4j
public class EfsFileSystemCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EfsFileSystem";

    private EfsClient efsClient;

    public EfsFileSystemCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "efs", "aws")).build());
        log.debug("EfsFileSystemCollector created");
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
        log.debug("EfsFileSystemCollector.collect called");

        if (this.efsClient == null)
        {
            this.efsClient = AwsClientFactory.getInstance().getEfsClient(providerContext);
        }

        try
        {
            DescribeFileSystemsRequest.Builder requestBuilder = DescribeFileSystemsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EfsFileSystemCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeFileSystemsResponse response = this.efsClient.describeFileSystems(requestBuilder.build());
            List<FileSystemDescription> fileSystems = response.fileSystems();
            String nextMarker = response.nextMarker();

            log.debug("EfsFileSystemCollector received {} file systems, nextMarker={}",
                fileSystems != null ? fileSystems.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (fileSystems != null)
            {
                for (FileSystemDescription fs : fileSystems)
                {
                    if (fs == null) continue;

                    String fsId   = fs.fileSystemId();
                    String fsArn  = fs.fileSystemArn();

                    Map<String, String> tags = fs.tags() == null ? new HashMap<>()
                        : fs.tags().stream().collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", fsId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("fileSystemId", fsId);
                    attributes.put("fileSystemArn", fsArn);
                    attributes.put("name", fs.name());
                    attributes.put("ownerId", fs.ownerId());
                    attributes.put("creationToken", fs.creationToken());
                    attributes.put("lifeCycleState", fs.lifeCycleStateAsString());
                    attributes.put("numberOfMountTargets", fs.numberOfMountTargets());
                    attributes.put("performanceMode", fs.performanceModeAsString());
                    attributes.put("encrypted", fs.encrypted());
                    attributes.put("kmsKeyId", fs.kmsKeyId());
                    attributes.put("throughputMode", fs.throughputModeAsString());
                    attributes.put("provisionedThroughputInMibps", fs.provisionedThroughputInMibps());
                    attributes.put("availabilityZoneName", fs.availabilityZoneName());
                    attributes.put("creationTime", fs.creationTime() != null ? fs.creationTime().toString() : null);
                    if (fs.sizeInBytes() != null)
                    {
                        attributes.put("sizeInBytes", fs.sizeInBytes().value());
                    }
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(fsArn != null ? fsArn : fsId)
                            .qualifiedResourceName(fsArn != null ? fsArn : fsId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(fs)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
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
        catch (EfsException e)
        {
            log.error("EfsFileSystemCollector EFS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EFS file systems", e);
        }
        catch (Exception e)
        {
            log.error("EfsFileSystemCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EFS file systems", e);
        }
    }
}
