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
import software.amazon.awssdk.services.storagegateway.StorageGatewayClient;
import software.amazon.awssdk.services.storagegateway.model.ListGatewaysRequest;
import software.amazon.awssdk.services.storagegateway.model.ListGatewaysResponse;
import software.amazon.awssdk.services.storagegateway.model.ListVolumesRequest;
import software.amazon.awssdk.services.storagegateway.model.ListVolumesResponse;
import software.amazon.awssdk.services.storagegateway.model.StorageGatewayException;
import software.amazon.awssdk.services.storagegateway.model.VolumeInfo;

/**
 * Collects Storage Gateway volumes and virtual tapes via the StorageGateway ListVolumes API.
 */
@Slf4j
public class StorageGatewayVolumeCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "StorageGatewayVolume";

    private StorageGatewayClient storageGatewayClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of("StorageGateway"))
            .tags(Set.of("storage", "storage-gateway", "volume", "aws"))
            .build();

    public StorageGatewayVolumeCollector()
    {
        log.debug("StorageGatewayVolumeCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("StorageGatewayVolumeCollector.collect called");

        if (this.storageGatewayClient == null)
        {
            this.storageGatewayClient = AwsClientFactory.getInstance().getStorageGatewayClient(providerContext);
        }

        try
        {
            // First, list all gateways to enumerate volumes per gateway
            List<String> gatewayArns = new ArrayList<>();
            String gwMarker = null;
            do
            {
                ListGatewaysRequest gwReq = ListGatewaysRequest.builder()
                    .marker(gwMarker)
                    .build();
                ListGatewaysResponse gwResp = this.storageGatewayClient.listGateways(gwReq);
                if (gwResp.gateways() != null)
                {
                    gwResp.gateways().forEach(gw -> gatewayArns.add(gw.gatewayARN()));
                }
                gwMarker = gwResp.marker();
            }
            while (gwMarker != null && !gwMarker.isBlank());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String gatewayArn : gatewayArns)
            {
                String volMarker = null;
                do
                {
                    ListVolumesRequest volReq = ListVolumesRequest.builder()
                        .gatewayARN(gatewayArn)
                        .marker(volMarker)
                        .build();
                    ListVolumesResponse volResp = this.storageGatewayClient.listVolumes(volReq);

                    if (volResp.volumeInfos() != null)
                    {
                        for (VolumeInfo vol : volResp.volumeInfos())
                        {
                            if (vol == null) continue;

                            String volArn = vol.volumeARN();
                            String volId  = vol.volumeId();

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("volumeId", volId);
                            attributes.put("volumeArn", volArn);
                            attributes.put("volumeType", vol.volumeType());
                            attributes.put("volumeSizeInBytes", vol.volumeSizeInBytes());
                            attributes.put("gatewayId", vol.gatewayId());
                            attributes.put("gatewayArn", vol.gatewayARN());

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(volArn != null ? volArn : volId)
                                    .qualifiedResourceName(volArn != null ? volArn : volId)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(volId)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(vol)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }
                    volMarker = volResp.marker();
                }
                while (volMarker != null && !volMarker.isBlank());
            }

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
        catch (StorageGatewayException e)
        {
            log.error("StorageGatewayVolumeCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Storage Gateway volumes", e);
        }
        catch (Exception e)
        {
            log.error("StorageGatewayVolumeCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Storage Gateway volumes", e);
        }
    }
}
