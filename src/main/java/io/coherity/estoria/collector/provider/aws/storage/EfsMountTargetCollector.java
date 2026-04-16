package io.coherity.estoria.collector.provider.aws.storage;

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
import software.amazon.awssdk.services.efs.EfsClient;
import software.amazon.awssdk.services.efs.model.DescribeMountTargetsRequest;
import software.amazon.awssdk.services.efs.model.DescribeMountTargetsResponse;
import software.amazon.awssdk.services.efs.model.EfsException;
import software.amazon.awssdk.services.efs.model.MountTargetDescription;

/**
 * Collects EFS mount targets via the EFS DescribeMountTargets API.
 */
@Slf4j
public class EfsMountTargetCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "EfsMountTarget";


    public EfsMountTargetCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("EfsFileSystem"), Set.of("storage", "efs", "mount-target", "aws")).build());
        log.debug("EfsMountTargetCollector created");
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
        log.debug("EfsMountTargetCollector.collect called");

        EfsClient efsClient = AwsClientFactory.getInstance().getEfsClient(providerContext);

        try
        {
            DescribeMountTargetsRequest.Builder requestBuilder = DescribeMountTargetsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("EfsMountTargetCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeMountTargetsResponse response = efsClient.describeMountTargets(requestBuilder.build());
            List<MountTargetDescription> mountTargets = response.mountTargets();
            String nextMarker = response.nextMarker();

            log.debug("EfsMountTargetCollector received {} mount targets, nextMarker={}",
                mountTargets != null ? mountTargets.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (mountTargets != null)
            {
                for (MountTargetDescription mt : mountTargets)
                {
                    if (mt == null) continue;

                    String mtId  = mt.mountTargetId();
                    String mtArn = mt.mountTargetId(); // EFS mount targets use ID as identifier

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("mountTargetId", mtId);
                    attributes.put("fileSystemId", mt.fileSystemId());
                    attributes.put("subnetId", mt.subnetId());
                    attributes.put("availabilityZoneName", mt.availabilityZoneName());
                    attributes.put("availabilityZoneId", mt.availabilityZoneId());
                    attributes.put("vpcId", mt.vpcId());
                    attributes.put("ipAddress", mt.ipAddress());
                    attributes.put("networkInterfaceId", mt.networkInterfaceId());
                    attributes.put("ownerId", mt.ownerId());
                    attributes.put("lifeCycleState", mt.lifeCycleStateAsString());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(mtId)
                            .qualifiedResourceName(mtId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(mtId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(mt)
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
            log.error("EfsMountTargetCollector EFS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EFS mount targets", e);
        }
        catch (Exception e)
        {
            log.error("EfsMountTargetCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EFS mount targets", e);
        }
    }
}
