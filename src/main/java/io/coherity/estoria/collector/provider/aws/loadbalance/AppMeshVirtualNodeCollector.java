package io.coherity.estoria.collector.provider.aws.loadbalance;

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
import software.amazon.awssdk.services.appmesh.AppMeshClient;
import software.amazon.awssdk.services.appmesh.model.AppMeshException;
import software.amazon.awssdk.services.appmesh.model.ListMeshesRequest;
import software.amazon.awssdk.services.appmesh.model.ListMeshesResponse;
import software.amazon.awssdk.services.appmesh.model.ListVirtualNodesRequest;
import software.amazon.awssdk.services.appmesh.model.ListVirtualNodesResponse;
import software.amazon.awssdk.services.appmesh.model.MeshRef;
import software.amazon.awssdk.services.appmesh.model.VirtualNodeRef;

/**
 * Collects AWS App Mesh virtual nodes across all meshes via ListVirtualNodes.
 */
@Slf4j
public class AppMeshVirtualNodeCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppMeshVirtualNode";


    public AppMeshVirtualNodeCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("loadbalance", "appmesh", "virtual-node", "aws")).build());
        log.debug("AppMeshVirtualNodeCollector created");
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
        log.debug("AppMeshVirtualNodeCollector.collect called");

        AppMeshClient appMeshClient = AwsClientFactory.getInstance().getAppMeshClient(providerContext);

        try
        {
            // Enumerate all meshes first
            List<String> meshNames = new ArrayList<>();
            String meshNextToken = null;
            do
            {
                ListMeshesRequest.Builder meshReqBuilder = ListMeshesRequest.builder();
                if (meshNextToken != null) meshReqBuilder.nextToken(meshNextToken);
                ListMeshesResponse meshResp = appMeshClient.listMeshes(meshReqBuilder.build());
                for (MeshRef m : meshResp.meshes())
                {
                    meshNames.add(m.meshName());
                }
                meshNextToken = meshResp.nextToken();
            }
            while (meshNextToken != null && !meshNextToken.isBlank());

            log.debug("AppMeshVirtualNodeCollector found {} meshes", meshNames.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String meshName : meshNames)
            {
                String vnNextToken = null;
                do
                {
                    ListVirtualNodesRequest.Builder vnReqBuilder =
                        ListVirtualNodesRequest.builder().meshName(meshName);
                    if (vnNextToken != null) vnReqBuilder.nextToken(vnNextToken);

                    ListVirtualNodesResponse vnResp =
                        appMeshClient.listVirtualNodes(vnReqBuilder.build());

                    for (VirtualNodeRef vn : vnResp.virtualNodes())
                    {
                        if (vn == null) continue;

                        String arn    = vn.arn();
                        String vnName = vn.virtualNodeName();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("virtualNodeName", vnName);
                        attributes.put("meshName", meshName);
                        attributes.put("arn", arn);
                        attributes.put("meshOwner", vn.meshOwner());
                        attributes.put("resourceOwner", vn.resourceOwner());
                        attributes.put("createdAt",
                            vn.createdAt() != null ? vn.createdAt().toString() : null);
                        attributes.put("lastUpdatedAt",
                            vn.lastUpdatedAt() != null ? vn.lastUpdatedAt().toString() : null);
                        attributes.put("version", vn.version());

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(arn != null ? arn : meshName + "/" + vnName)
                                .qualifiedResourceName(arn != null ? arn : meshName + "/" + vnName)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(vnName)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(vn)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                    vnNextToken = vnResp.nextToken();
                }
                while (vnNextToken != null && !vnNextToken.isBlank());
            }

            log.debug("AppMeshVirtualNodeCollector collected {} virtual nodes", entities.size());

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
        catch (AppMeshException e)
        {
            log.error("AppMeshVirtualNodeCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect App Mesh virtual nodes", e);
        }
        catch (Exception e)
        {
            log.error("AppMeshVirtualNodeCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting App Mesh virtual nodes", e);
        }
    }
}
