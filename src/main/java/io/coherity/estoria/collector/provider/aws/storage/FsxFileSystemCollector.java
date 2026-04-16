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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.fsx.FSxClient;
import software.amazon.awssdk.services.fsx.model.DescribeFileSystemsRequest;
import software.amazon.awssdk.services.fsx.model.DescribeFileSystemsResponse;
import software.amazon.awssdk.services.fsx.model.FileSystem;
import software.amazon.awssdk.services.fsx.model.Tag;

/**
 * Collects FSx file systems (Lustre, Windows, ONTAP, OpenZFS) via the FSx DescribeFileSystems API.
 */
@Slf4j
public class FsxFileSystemCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "FsxFileSystem";


    public FsxFileSystemCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "fsx", "aws")).build());
        log.debug("FsxFileSystemCollector created");
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
        log.debug("FsxFileSystemCollector.collect called");

        FSxClient fsxClient = AwsClientFactory.getInstance().getFsxClient(providerContext);

        try
        {
            DescribeFileSystemsRequest.Builder requestBuilder = DescribeFileSystemsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("FsxFileSystemCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeFileSystemsResponse response = fsxClient.describeFileSystems(requestBuilder.build());
            List<FileSystem> fileSystems = response.fileSystems();
            String nextToken = response.nextToken();

            log.debug("FsxFileSystemCollector received {} FSx file systems, nextToken={}",
                fileSystems != null ? fileSystems.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (fileSystems != null)
            {
                for (FileSystem fs : fileSystems)
                {
                    if (fs == null) continue;

                    String fsId  = fs.fileSystemId();
                    String fsArn = fs.resourceARN();

                    Map<String, String> tags = fs.tags() == null ? new HashMap<>()
                        : fs.tags().stream().collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", fsId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("fileSystemId", fsId);
                    attributes.put("fileSystemArn", fsArn);
                    attributes.put("fileSystemType", fs.fileSystemTypeAsString());
                    attributes.put("fileSystemTypeVersion", fs.fileSystemTypeVersion());
                    attributes.put("lifecycle", fs.lifecycleAsString());
                    attributes.put("storageCapacity", fs.storageCapacity());
                    attributes.put("storageType", fs.storageTypeAsString());
                    attributes.put("vpcId", fs.vpcId());
                    attributes.put("ownerId", fs.ownerId());
                    attributes.put("dnsName", fs.dnsName());
                    attributes.put("kmsKeyId", fs.kmsKeyId());
                    attributes.put("creationTime", fs.creationTime() != null ? fs.creationTime().toString() : null);
                    attributes.put("subnetIds", fs.subnetIds());
                    attributes.put("networkInterfaceIds", fs.networkInterfaceIds());
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
        catch (AwsServiceException e)
        {
            log.error("FsxFileSystemCollector FSx error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect FSx file systems", e);
        }
        catch (Exception e)
        {
            log.error("FsxFileSystemCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting FSx file systems", e);
        }
    }
}
