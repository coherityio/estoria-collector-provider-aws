package io.coherity.estoria.collector.provider.aws.integration;

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
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;
import software.amazon.awssdk.services.lakeformation.model.ListResourcesRequest;
import software.amazon.awssdk.services.lakeformation.model.ListResourcesResponse;
import software.amazon.awssdk.services.lakeformation.model.ResourceInfo;

/**
 * Collects Lake Formation registered resources via the ListResources API.
 */
@Slf4j
public class LakeFormationResourceCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "LakeFormationResource";

    private LakeFormationClient lakeFormationClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("integration", "lake-formation", "governance", "aws"))
            .build();

    public LakeFormationResourceCollector()
    {
        log.debug("LakeFormationResourceCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
        log.debug("LakeFormationResourceCollector.collectEntities called");

        if (this.lakeFormationClient == null)
        {
            this.lakeFormationClient = AwsClientFactory.getInstance().getLakeFormationClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListResourcesRequest.Builder requestBuilder = ListResourcesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("LakeFormationResourceCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListResourcesResponse response = this.lakeFormationClient.listResources(requestBuilder.build());
            List<ResourceInfo> resourceList = response.resourceInfoList();
            String             nextToken    = response.nextToken();

            log.debug("LakeFormationResourceCollector received {} resources, nextToken={}",
                resourceList != null ? resourceList.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (resourceList != null)
            {
                for (ResourceInfo resourceInfo : resourceList)
                {
                    if (resourceInfo == null) continue;

                    String resourceArn = resourceInfo.resourceArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("resourceArn",       resourceArn);
                    attributes.put("roleArn",           resourceInfo.roleArn());
                    attributes.put("lastModified",
                        resourceInfo.lastModified() != null ? resourceInfo.lastModified().toString() : null);
                    attributes.put("hybridAccessEnabled", resourceInfo.hybridAccessEnabled());
                    attributes.put("withFederation",      resourceInfo.withFederation());
                    attributes.put("accountId",           accountId);
                    attributes.put("region",              region);

                    String resourceName = resourceArn != null && resourceArn.contains("/")
                        ? resourceArn.substring(resourceArn.lastIndexOf('/') + 1)
                        : resourceArn;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(resourceArn)
                            .qualifiedResourceName(resourceArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(resourceName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(resourceInfo)
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
        catch (LakeFormationException e)
        {
            log.error("LakeFormationResourceCollector Lake Formation error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Lake Formation resources", e);
        }
        catch (Exception e)
        {
            log.error("LakeFormationResourceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Lake Formation resources", e);
        }
    }
}
