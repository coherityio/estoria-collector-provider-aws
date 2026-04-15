package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.ec2.model.Volume;
import software.amazon.awssdk.services.ec2.model.VolumeAttachment;

/**
 * Collects EBS volume attachments by iterating all volumes and flattening their
 * attachment records. Each attachment represents a volume-to-instance binding.
 */
@Slf4j
public class EbsVolumeAttachmentCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EbsVolumeAttachment";

    private Ec2Client ec2Client;

    public EbsVolumeAttachmentCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("compute", "ebs", "volume", "attachment", "aws")).build());
        log.debug("EbsVolumeAttachmentCollector created");
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
        log.debug("EbsVolumeAttachmentCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();
            String pageNextToken = null;

            // Paginate through all volumes to collect their attachments
            do
            {
                DescribeVolumesRequest.Builder reqBuilder = DescribeVolumesRequest.builder();
                if (pageNextToken != null) reqBuilder.nextToken(pageNextToken);

                int pageSize = collectorRequestParams.getPageSize();
                if (pageSize > 0) reqBuilder.maxResults(pageSize);

                DescribeVolumesResponse response = this.ec2Client.describeVolumes(reqBuilder.build());

                for (Volume volume : response.volumes())
                {
                    if (volume == null || volume.attachments() == null) continue;

                    String volumeId = volume.volumeId();
                    String volumeArn = ARNHelper.ec2VolumeArn(region, accountId, volumeId);

                    for (VolumeAttachment attachment : volume.attachments())
                    {
                        if (attachment == null) continue;

                        String instanceId = attachment.instanceId();
                        // Synthetic ID: volumeId + ":" + instanceId + ":" + device
                        String syntheticId = volumeId + ":" + instanceId + ":" + attachment.device();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("volumeId", volumeId);
                        attributes.put("volumeArn", volumeArn);
                        attributes.put("instanceId", instanceId);
                        attributes.put("device", attachment.device());
                        attributes.put("state", attachment.stateAsString());
                        attributes.put("deleteOnTermination", attachment.deleteOnTermination());
                        attributes.put("attachTime",
                            attachment.attachTime() != null ? attachment.attachTime().toString() : null);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(syntheticId)
                                .qualifiedResourceName(syntheticId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(volumeId + " -> " + instanceId)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(attachment)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                }

                pageNextToken = response.nextToken();
            }
            while (pageNextToken != null && !pageNextToken.isBlank());

            log.debug("EbsVolumeAttachmentCollector collected {} attachments", entities.size());

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
        catch (Ec2Exception e)
        {
            log.error("EbsVolumeAttachmentCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EBS volume attachments", e);
        }
        catch (Exception e)
        {
            log.error("EbsVolumeAttachmentCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EBS volume attachments", e);
        }
    }

}
