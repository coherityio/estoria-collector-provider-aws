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
import software.amazon.awssdk.services.budgets.BudgetsClient;
import software.amazon.awssdk.services.budgets.model.Budget;
import software.amazon.awssdk.services.budgets.model.BudgetsException;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetsRequest;
import software.amazon.awssdk.services.budgets.model.DescribeBudgetsResponse;

/**
 * Collects AWS Budgets definitions via the Budgets DescribeBudgets API.
 */
@Slf4j
public class BudgetsBudgetCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "BudgetsBudget";


    public BudgetsBudgetCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("account", "budgets", "cost", "aws")).build());
        log.debug("BudgetsBudgetCollector created");
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
        log.debug("BudgetsBudgetCollector.collectEntities called");

        BudgetsClient budgetsClient = AwsClientFactory.getInstance().getBudgetsClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();

            DescribeBudgetsRequest.Builder requestBuilder = DescribeBudgetsRequest.builder()
                .accountId(accountId);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("BudgetsBudgetCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeBudgetsResponse response = budgetsClient.describeBudgets(requestBuilder.build());
            List<Budget> budgets = response.budgets();
            String nextToken = response.nextToken();

            log.debug("BudgetsBudgetCollector received {} budgets, nextToken={}",
                budgets != null ? budgets.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (budgets != null)
            {
                for (Budget budget : budgets)
                {
                    if (budget == null) continue;

                    String budgetName = budget.budgetName();
                    String syntheticId = "arn:aws:budgets::" + accountId + ":budget/" + budgetName;

                    List<String> costFiltersKeys = budget.costFilters() == null ? List.of()
                        : new ArrayList<>(budget.costFilters().keySet());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("budgetName", budgetName);
                    attributes.put("accountId", accountId);
                    attributes.put("budgetType", budget.budgetTypeAsString());
                    attributes.put("timeUnit", budget.timeUnitAsString());
                    attributes.put("budgetLimit",
                        budget.budgetLimit() != null ? budget.budgetLimit().amount() + " " + budget.budgetLimit().unit() : null);
                    attributes.put("calculatedSpend",
                        budget.calculatedSpend() != null && budget.calculatedSpend().actualSpend() != null
                            ? budget.calculatedSpend().actualSpend().amount() + " " + budget.calculatedSpend().actualSpend().unit()
                            : null);
                    attributes.put("timePeriodStart",
                        budget.timePeriod() != null && budget.timePeriod().start() != null
                            ? budget.timePeriod().start().toString() : null);
                    attributes.put("timePeriodEnd",
                        budget.timePeriod() != null && budget.timePeriod().end() != null
                            ? budget.timePeriod().end().toString() : null);
                    attributes.put("lastUpdatedTime",
                        budget.lastUpdatedTime() != null ? budget.lastUpdatedTime().toString() : null);
                    attributes.put("costFilterKeys", costFiltersKeys);
                    attributes.put("includeCredit",
                        budget.costTypes() != null ? budget.costTypes().includeCredit() : null);
                    attributes.put("includeDiscount",
                        budget.costTypes() != null ? budget.costTypes().includeDiscount() : null);
                    attributes.put("includeTax",
                        budget.costTypes() != null ? budget.costTypes().includeTax() : null);
                    attributes.put("includeSupport",
                        budget.costTypes() != null ? budget.costTypes().includeSupport() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(syntheticId)
                            .qualifiedResourceName(syntheticId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(budgetName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(budget)
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
        catch (BudgetsException e)
        {
            log.error("BudgetsBudgetCollector Budgets error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Budgets budgets", e);
        }
        catch (Exception e)
        {
            log.error("BudgetsBudgetCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Budgets budgets", e);
        }
    }
}
