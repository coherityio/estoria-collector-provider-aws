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
import software.amazon.awssdk.services.appmesh.model.ListVirtualServicesRequest;
import software.amazon.awssdk.services.appmesh.model.ListVirtualServicesResponse;
import software.amazon.awssdk.services.appmesh.model.MeshRef;
import software.amazon.awssdk.services.appmesh.model.VirtualServiceRef;

/**
 * Collects AWS App Mesh virtual services across all meshes via ListVirtualServices.
 */
@Slf4j
public class AppMeshVirtualServiceCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "AppMeshVirtualService";

    private AppMeshClient appMeshClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "appmesh", "virtual-service", "aws"))
            .build();

    public AppMeshVirtualServiceCollector()
    {
        log.debug("AppMeshVirtualServiceCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("AppMeshVirtualServiceCollector.collect called");

        if (this.appMeshClient == null)
        {
            this.appMeshClient = AwsClientFactory.getInstance().getAppMeshClient(providerContext);
        }

        try
        {
            // First enumerate all meshes, then list virtual services per mesh
            List<String> meshNames = new ArrayList<>();
            String meshNextToken = null;
            do
            {
                ListMeshesRequest.Builder meshReqBuilder = ListMeshesRequest.builder();
                if (meshNextToken != null) meshReqBuilder.nextToken(meshNextToken);
                ListMeshesResponse meshResp = this.appMeshClient.listMeshes(meshReqBuilder.build());
                for (MeshRef m : meshResp.meshes())
                {
                    meshNames.add(m.meshName());
                }
                meshNextToken = meshResp.nextToken();
            }
            while (meshNextToken != null && !meshNextToken.isBlank());

            log.debug("AppMeshVirtualServiceCollector found {} meshes", meshNames.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String meshName : meshNames)
            {
                String vsNextToken = null;
                do
                {
                    ListVirtualServicesRequest.Builder vsReqBuilder =
                        ListVirtualServicesRequest.builder().meshName(meshName);
                    if (vsNextToken != null) vsReqBuilder.nextToken(vsNextToken);

                    ListVirtualServicesResponse vsResp =
                        this.appMeshClient.listVirtualServices(vsReqBuilder.build());

                    for (VirtualServiceRef vs : vsResp.virtualServices())
                    {
                        if (vs == null) continue;

                        String arn = vs.arn();
                        String vsName = vs.virtualServiceName();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("virtualServiceName", vsName);
                        attributes.put("meshName", meshName);
                        attributes.put("arn", arn);
                        attributes.put("meshOwner", vs.meshOwner());
                        attributes.put("resourceOwner", vs.resourceOwner());
                        attributes.put("createdAt",
                            vs.createdAt() != null ? vs.createdAt().toString() : null);
                        attributes.put("lastUpdatedAt",
                            vs.lastUpdatedAt() != null ? vs.lastUpdatedAt().toString() : null);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(arn != null ? arn : meshName + "/" + vsName)
                                .qualifiedResourceName(arn != null ? arn : meshName + "/" + vsName)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(vsName)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(vs)
                            .collectedAt(now)
                            .build();

                        entities.add(entity);
                    }
                    vsNextToken = vsResp.nextToken();
                }
                while (vsNextToken != null && !vsNextToken.isBlank());
            }

            log.debug("AppMeshVirtualServiceCollector collected {} virtual services", entities.size());

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
            log.error("AppMeshVirtualServiceCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect App Mesh virtual services", e);
        }
        catch (Exception e)
        {
            log.error("AppMeshVirtualServiceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting App Mesh virtual services", e);
        }
    }
}
