package io.coherity.estoria.collector.provider.aws.account;

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
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.costexplorer.model.CostCategory;
import software.amazon.awssdk.services.costexplorer.model.CostCategoryReference;
import software.amazon.awssdk.services.costexplorer.model.CostExplorerException;
import software.amazon.awssdk.services.costexplorer.model.ListCostCategoryDefinitionsRequest;
import software.amazon.awssdk.services.costexplorer.model.ListCostCategoryDefinitionsResponse;

/**
 * Collects AWS Cost Explorer cost category definitions
 * via the CostExplorer ListCostCategoryDefinitions API.
 */
@Slf4j
public class CostCategoryCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CostCategory";


    public CostCategoryCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("account", "cost", "billing", "aws")).build());
        log.debug("CostCategoryCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("CostCategoryCollector.collectEntities called");

        CostExplorerClient costExplorerClient = AwsClientFactory.getInstance().getCostExplorerClient(providerContext);

        try
        {
            ListCostCategoryDefinitionsRequest.Builder requestBuilder =
                ListCostCategoryDefinitionsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CostCategoryCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListCostCategoryDefinitionsResponse response =
                costExplorerClient.listCostCategoryDefinitions(requestBuilder.build());

            List<CostCategoryReference> categories =
            	    response.costCategoryReferences() == null ? List.of()
            	        : response.costCategoryReferences();            

            String nextToken = response.nextToken();

            log.debug("CostCategoryCollector received {} cost categories, nextToken={}",
                categories.size(), nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (CostCategoryReference category : categories)
            {
                if (category == null) continue;

                String arn  = category.costCategoryArn();
                String name = category.name();

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("costCategoryArn", arn);
                attributes.put("name", name);
                attributes.put("effectiveStart", category.effectiveStart());
                attributes.put("effectiveEnd", category.effectiveEnd());
                attributes.put("numberOfRules", category.numberOfRules());
                attributes.put("defaultValue", category.defaultValue());
                attributes.put("values", category.values());
                attributes.put("processingStatus",
                    category.processingStatus() == null ? null
                        : category.processingStatus().stream()
                            .map(s -> s.componentAsString() + "=" + s.statusAsString())
                            .collect(Collectors.toList()));

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(arn)
                        .qualifiedResourceName(arn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(name)
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(category)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
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
        catch (CostExplorerException e)
        {
            log.error("CostCategoryCollector CostExplorer error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect cost categories", e);
        }
        catch (Exception e)
        {
            log.error("CostCategoryCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting cost categories", e);
        }
    }
}
