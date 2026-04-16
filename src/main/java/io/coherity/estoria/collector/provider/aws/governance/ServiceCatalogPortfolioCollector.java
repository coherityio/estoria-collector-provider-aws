package io.coherity.estoria.collector.provider.aws.governance;

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
import software.amazon.awssdk.services.servicecatalog.ServiceCatalogClient;
import software.amazon.awssdk.services.servicecatalog.model.ListPortfoliosRequest;
import software.amazon.awssdk.services.servicecatalog.model.ListPortfoliosResponse;
import software.amazon.awssdk.services.servicecatalog.model.PortfolioDetail;
import software.amazon.awssdk.services.servicecatalog.model.ServiceCatalogException;
import software.amazon.awssdk.services.servicecatalog.model.Tag;

/**
 * Collects Service Catalog portfolios via the ListPortfolios API.
 */
@Slf4j
public class ServiceCatalogPortfolioCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "ServiceCatalogPortfolio";


	public ServiceCatalogPortfolioCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("governance", "service-catalog", "aws")).build());
		log.debug("ServiceCatalogPortfolioCollector created");
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
		log.debug("ServiceCatalogPortfolioCollector.collectEntities called");

		ServiceCatalogClient serviceCatalogClient = AwsClientFactory.getInstance().getServiceCatalogClient(providerContext);

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListPortfoliosRequest.Builder requestBuilder = ListPortfoliosRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("ServiceCatalogPortfolioCollector resuming from pageToken: {}", token);
				requestBuilder.pageToken(token);
			});

			ListPortfoliosResponse response = serviceCatalogClient.listPortfolios(requestBuilder.build());
			List<PortfolioDetail> portfolios = response.portfolioDetails();
			String                nextToken  = response.nextPageToken();

			log.debug("ServiceCatalogPortfolioCollector received {} portfolios, nextToken={}",
				portfolios != null ? portfolios.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (portfolios != null)
			{
				for (PortfolioDetail portfolio : portfolios)
				{
					if (portfolio == null) continue;

					String portfolioId  = portfolio.id();
					String portfolioArn = portfolio.arn() != null ? portfolio.arn()
						: "arn:aws:catalog:" + region + ":" + accountId + ":portfolio/" + portfolioId;

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("portfolioId",          portfolioId);
					attributes.put("portfolioArn",         portfolioArn);
					attributes.put("portfolioName",        portfolio.displayName());
					attributes.put("description",          portfolio.description());
					attributes.put("providerName",         portfolio.providerName());
					attributes.put("createdTime",
						portfolio.createdTime() != null ? portfolio.createdTime().toString() : null);
					attributes.put("accountId",            accountId);
					attributes.put("region",               region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(portfolioArn)
							.qualifiedResourceName(portfolioArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(portfolio.displayName() != null ? portfolio.displayName() : portfolioId)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(portfolio)
						.collectedAt(now)
						.build();

					entities.add(entity);
				}
			}

			String finalNextToken = nextToken;
			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken()
				{
					return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
				}
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (ServiceCatalogException e)
		{
			log.error("ServiceCatalogPortfolioCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Service Catalog portfolios", e);
		}
		catch (Exception e)
		{
			log.error("ServiceCatalogPortfolioCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Service Catalog portfolios", e);
		}
	}
}
