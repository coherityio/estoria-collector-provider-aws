package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVolumesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.Volume;

/**
 * Collects EBS volumes via the EC2 DescribeVolumes API.
 */
@Slf4j
public class EbsVolumeCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EbsVolume";


    public EbsVolumeCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("compute", "ebs", "volume", "storage", "aws")).build());
        log.debug("EbsVolumeCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.ACCOUNT_REGIONAL;
    }

    @Override
    public EntityCategory getEntityCategory()
    {
        return EntityCategory.RESOURCE;
    }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("EbsVolumeCollector.collect called");

        Ec2Client ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            DescribeVolumesRequest.Builder requestBuilder = DescribeVolumesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EbsVolumeCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeVolumesResponse response = ec2Client.describeVolumes(requestBuilder.build());
            List<Volume> volumes = response.volumes();
            String nextToken = response.nextToken();

            log.debug("EbsVolumeCollector received {} volumes, nextToken={}",
                volumes != null ? volumes.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (volumes != null)
            {
                for (Volume volume : volumes)
                {
                    if (volume == null) continue;

                    String volumeId = volume.volumeId();
                    String arn = ARNHelper.ec2VolumeArn(region, accountId, volumeId);

                    Map<String, String> tags = volume.tags() == null ? Map.of()
                        : volume.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("volumeId", volumeId);
                    attributes.put("state", volume.stateAsString());
                    attributes.put("size", volume.size());
                    attributes.put("volumeType", volume.volumeTypeAsString());
                    attributes.put("availabilityZone", volume.availabilityZone());
                    attributes.put("encrypted", volume.encrypted());
                    attributes.put("kmsKeyId", volume.kmsKeyId());
                    attributes.put("iops", volume.iops());
                    attributes.put("throughput", volume.throughput());
                    attributes.put("snapshotId", volume.snapshotId());
                    attributes.put("multiAttachEnabled", volume.multiAttachEnabled());
                    attributes.put("createTime",
                        volume.createTime() != null ? volume.createTime().toString() : null);
                    attributes.put("tags", tags);

                    String name = tags.getOrDefault("Name", volumeId);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(volumeId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(volume)
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
        catch (Ec2Exception e)
        {
            log.error("EbsVolumeCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EBS volumes", e);
        }
        catch (Exception e)
        {
            log.error("EbsVolumeCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EBS volumes", e);
        }
    }

}
