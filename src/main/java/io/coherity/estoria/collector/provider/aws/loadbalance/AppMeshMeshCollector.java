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
import software.amazon.awssdk.services.appmesh.model.MeshRef;

/**
 * Collects AWS App Mesh meshes via the ListMeshes API.
 */
@Slf4j
public class AppMeshMeshCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "AppMeshMesh";

    private AppMeshClient appMeshClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "appmesh", "mesh", "aws"))
            .build();

    public AppMeshMeshCollector()
    {
        log.debug("AppMeshMeshCollector created");
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
        log.debug("AppMeshMeshCollector.collect called");

        if (this.appMeshClient == null)
        {
            this.appMeshClient = AwsClientFactory.getInstance().getAppMeshClient(providerContext);
        }

        try
        {
            ListMeshesRequest.Builder requestBuilder = ListMeshesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("AppMeshMeshCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListMeshesResponse response = this.appMeshClient.listMeshes(requestBuilder.build());
            List<MeshRef> meshes = response.meshes();
            String nextToken = response.nextToken();

            log.debug("AppMeshMeshCollector received {} meshes, nextToken={}",
                meshes != null ? meshes.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (meshes != null)
            {
                for (MeshRef mesh : meshes)
                {
                    if (mesh == null) continue;

                    String meshName = mesh.meshName();
                    String arn      = mesh.arn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("meshName", meshName);
                    attributes.put("arn", arn);
                    attributes.put("meshOwner", mesh.meshOwner());
                    attributes.put("resourceOwner", mesh.resourceOwner());
                    attributes.put("createdAt",
                        mesh.createdAt() != null ? mesh.createdAt().toString() : null);
                    attributes.put("lastUpdatedAt",
                        mesh.lastUpdatedAt() != null ? mesh.lastUpdatedAt().toString() : null);
                    attributes.put("version", mesh.version());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : meshName)
                            .qualifiedResourceName(arn != null ? arn : meshName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(meshName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(mesh)
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
        catch (AppMeshException e)
        {
            log.error("AppMeshMeshCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect App Mesh meshes", e);
        }
        catch (Exception e)
        {
            log.error("AppMeshMeshCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting App Mesh meshes", e);
        }
    }
}
