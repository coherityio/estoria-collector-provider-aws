package io.coherity.estoria.collector.provider.aws.governance;

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
import software.amazon.awssdk.services.servicecatalog.ServiceCatalogClient;
import software.amazon.awssdk.services.servicecatalog.model.ProductViewDetail;
import software.amazon.awssdk.services.servicecatalog.model.ProductViewSummary;
import software.amazon.awssdk.services.servicecatalog.model.SearchProductsAsAdminRequest;
import software.amazon.awssdk.services.servicecatalog.model.SearchProductsAsAdminResponse;
import software.amazon.awssdk.services.servicecatalog.model.ServiceCatalogException;

/**
 * Collects Service Catalog products via the SearchProductsAsAdmin API.
 */
@Slf4j
public class ServiceCatalogProductCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "ServiceCatalogProduct";

	private ServiceCatalogClient serviceCatalogClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of(ServiceCatalogPortfolioCollector.ENTITY_TYPE))
			.tags(Set.of("governance", "service-catalog", "aws"))
			.build();

	public ServiceCatalogProductCollector()
	{
		log.debug("ServiceCatalogProductCollector created");
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
		log.debug("ServiceCatalogProductCollector.collectEntities called");

		if (this.serviceCatalogClient == null)
		{
			this.serviceCatalogClient = AwsClientFactory.getInstance().getServiceCatalogClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			SearchProductsAsAdminRequest.Builder requestBuilder = SearchProductsAsAdminRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("ServiceCatalogProductCollector resuming from pageToken: {}", token);
				requestBuilder.pageToken(token);
			});

			SearchProductsAsAdminResponse response =
				this.serviceCatalogClient.searchProductsAsAdmin(requestBuilder.build());
			List<ProductViewDetail> products  = response.productViewDetails();
			String                  nextToken = response.nextPageToken();

			log.debug("ServiceCatalogProductCollector received {} products, nextToken={}",
				products != null ? products.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (products != null)
			{
				for (ProductViewDetail pvd : products)
				{
					if (pvd == null) continue;

					ProductViewSummary pvs = pvd.productViewSummary();
					if (pvs == null) continue;

					String productId  = pvs.productId();
					String productArn = pvd.productARN() != null ? pvd.productARN()
						: "arn:aws:catalog:" + region + ":" + accountId + ":product/" + productId;

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("productId",       productId);
					attributes.put("productArn",      productArn);
					attributes.put("productName",     pvs.name());
					attributes.put("productType",
						pvs.type() != null ? pvs.type().toString() : null);
					attributes.put("owner",           pvs.owner());
					attributes.put("shortDescription", pvs.shortDescription());
					attributes.put("distributor",     pvs.distributor());
					attributes.put("hasDefaultPath",  pvs.hasDefaultPath());
					attributes.put("supportEmail",    pvs.supportEmail());
					attributes.put("supportUrl",      pvs.supportUrl());
					attributes.put("status",
						pvd.status() != null ? pvd.status().toString() : null);
					attributes.put("createdTime",
						pvd.createdTime() != null ? pvd.createdTime().toString() : null);
					attributes.put("accountId",       accountId);
					attributes.put("region",          region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(productArn)
							.qualifiedResourceName(productArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(pvs.name() != null ? pvs.name() : productId)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(pvd)
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
			log.error("ServiceCatalogProductCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Service Catalog products", e);
		}
		catch (Exception e)
		{
			log.error("ServiceCatalogProductCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Service Catalog products", e);
		}
	}
}
