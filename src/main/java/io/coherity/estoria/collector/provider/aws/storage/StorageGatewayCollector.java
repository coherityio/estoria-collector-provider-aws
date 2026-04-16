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
import software.amazon.awssdk.services.storagegateway.StorageGatewayClient;
import software.amazon.awssdk.services.storagegateway.model.GatewayInfo;
import software.amazon.awssdk.services.storagegateway.model.ListGatewaysRequest;
import software.amazon.awssdk.services.storagegateway.model.ListGatewaysResponse;
import software.amazon.awssdk.services.storagegateway.model.StorageGatewayException;

/**
 * Collects Storage Gateway appliances via the StorageGateway ListGateways API.
 */
@Slf4j
public class StorageGatewayCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "StorageGateway";


    public StorageGatewayCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "storage-gateway", "aws")).build());
        log.debug("StorageGatewayCollector created");
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
        log.debug("StorageGatewayCollector.collect called");

        StorageGatewayClient storageGatewayClient = AwsClientFactory.getInstance().getStorageGatewayClient(providerContext);

        try
        {
            ListGatewaysRequest.Builder requestBuilder = ListGatewaysRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("StorageGatewayCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListGatewaysResponse response = storageGatewayClient.listGateways(requestBuilder.build());
            List<GatewayInfo> gateways = response.gateways();
            String nextMarker = response.marker();

            log.debug("StorageGatewayCollector received {} gateways, nextMarker={}",
                gateways != null ? gateways.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (gateways != null)
            {
                for (GatewayInfo gw : gateways)
                {
                    if (gw == null) continue;

                    String gwArn  = gw.gatewayARN();
                    String gwId   = gw.gatewayId();
                    String gwName = gw.gatewayName();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("gatewayId", gwId);
                    attributes.put("gatewayArn", gwArn);
                    attributes.put("gatewayName", gwName);
                    attributes.put("gatewayType", gw.gatewayType());
                    attributes.put("gatewayOperationalState", gw.gatewayOperationalState());
                    attributes.put("ec2InstanceId", gw.ec2InstanceId());
                    attributes.put("ec2InstanceRegion", gw.ec2InstanceRegion());
                    attributes.put("hostEnvironment", gw.hostEnvironmentAsString());
                    attributes.put("hostEnvironmentId", gw.hostEnvironmentId());
                    attributes.put("deprecationDate", gw.deprecationDate());
                    attributes.put("softwareVersion", gw.softwareVersion());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(gwArn != null ? gwArn : gwId)
                            .qualifiedResourceName(gwArn != null ? gwArn : gwId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(gwName != null ? gwName : gwId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(gw)
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
        catch (StorageGatewayException e)
        {
            log.error("StorageGatewayCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Storage Gateways", e);
        }
        catch (Exception e)
        {
            log.error("StorageGatewayCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Storage Gateways", e);
        }
    }
}
