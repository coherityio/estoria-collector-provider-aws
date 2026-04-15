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
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetResourcePolicyRequest;
import software.amazon.awssdk.services.glue.model.GetResourcePolicyResponse;
import software.amazon.awssdk.services.glue.model.GlueException;

/**
 * Collects the Glue Data Catalog resource policy via the Glue GetResourcePolicy API.
 * Emits a single entity per account/region representing the catalog-level policy.
 */
@Slf4j
public class GlueDataCatalogResourcePolicyCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "GlueDataCatalogResourcePolicy";

    private GlueClient glueClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("integration", "glue", "data-catalog", "policy", "aws"))
            .build();

    public GlueDataCatalogResourcePolicyCollector()
    {
        log.debug("GlueDataCatalogResourcePolicyCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.REFERENCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("GlueDataCatalogResourcePolicyCollector.collectEntities called");

        if (this.glueClient == null)
        {
            this.glueClient = AwsClientFactory.getInstance().getGlueClient(providerContext);
        }

        List<CloudEntity> entities = new ArrayList<>();
        Instant now = Instant.now();

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetResourcePolicyResponse response =
                this.glueClient.getResourcePolicy(GetResourcePolicyRequest.builder().build());

            String policyJson       = response.policyInJson();
            String policyHash       = response.policyHash();
            String createTime       = response.createTime() != null ? response.createTime().toString() : null;
            String updateTime       = response.updateTime() != null ? response.updateTime().toString() : null;

            // Synthetic ARN for the catalog-level policy
            String catalogArn = "arn:aws:glue:" + region + ":" + accountId + ":catalog";

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("catalogArn",    catalogArn);
            attributes.put("policyJson",    policyJson);
            attributes.put("policyHash",    policyHash);
            attributes.put("createTime",    createTime);
            attributes.put("updateTime",    updateTime);
            attributes.put("accountId",     accountId);
            attributes.put("region",        region);

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id(catalogArn)
                    .qualifiedResourceName(catalogArn)
                    .build())
                .entityType(ENTITY_TYPE)
                .name("GlueDataCatalogPolicy-" + accountId + "-" + region)
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(response)
                .collectedAt(now)
                .build();

            entities.add(entity);
            log.debug("GlueDataCatalogResourcePolicyCollector collected catalog policy for account {}", accountId);
        }
        catch (EntityNotFoundException e)
        {
            // No resource policy set on this catalog — return empty result
            log.debug("GlueDataCatalogResourcePolicyCollector: no resource policy found, returning empty");
        }
        catch (GlueException e)
        {
            log.error("GlueDataCatalogResourcePolicyCollector Glue error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Glue Data Catalog resource policy", e);
        }
        catch (Exception e)
        {
            log.error("GlueDataCatalogResourcePolicyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Glue Data Catalog resource policy", e);
        }

        Map<String, Object> metadataValues = new HashMap<>();
        metadataValues.put("count", entities.size());

        List<CloudEntity> finalEntities = entities;
        return new CollectorCursor()
        {
            @Override public List<CloudEntity> getEntities() { return finalEntities; }
            @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
            @Override public CursorMetadata getMetadata()
            {
                return CursorMetadata.builder().values(metadataValues).build();
            }
        };
    }
}
