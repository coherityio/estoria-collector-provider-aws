package io.coherity.estoria.collector.provider.aws.governance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.services.servicecatalog.model.ProvisionedProductDetail;
import software.amazon.awssdk.services.servicecatalog.model.ScanProvisionedProductsRequest;
import software.amazon.awssdk.services.servicecatalog.model.ScanProvisionedProductsResponse;
import software.amazon.awssdk.services.servicecatalog.model.ServiceCatalogException;

/**
 * Collects Service Catalog provisioned products via the ScanProvisionedProducts API.
 */
@Slf4j
public class ServiceCatalogProvisionedProductCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "ServiceCatalogProvisionedProduct";

	private ServiceCatalogClient serviceCatalogClient;

	public ServiceCatalogProvisionedProductCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(ServiceCatalogProductCollector.ENTITY_TYPE), Set.of("governance", "service-catalog", "aws")).build());
		log.debug("ServiceCatalogProvisionedProductCollector created");
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
		log.debug("ServiceCatalogProvisionedProductCollector.collectEntities called");

		if (this.serviceCatalogClient == null)
		{
			this.serviceCatalogClient = AwsClientFactory.getInstance().getServiceCatalogClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ScanProvisionedProductsRequest.Builder requestBuilder = ScanProvisionedProductsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("ServiceCatalogProvisionedProductCollector resuming from pageToken: {}", token);
				requestBuilder.pageToken(token);
			});

			ScanProvisionedProductsResponse response =
				this.serviceCatalogClient.scanProvisionedProducts(requestBuilder.build());
			//List<ProvisionedProductAttribute> products  = response.provisionedProducts();

			List<ProvisionedProductDetail> products = response.provisionedProducts();			

			String                            nextToken = response.nextPageToken();

			log.debug("ServiceCatalogProvisionedProductCollector received {} provisioned products, nextToken={}",
				products != null ? products.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (products != null)
			{
				for (ProvisionedProductDetail pp : products)
				{
				    String ppId  = pp.id();
				    String ppArn = pp.arn() != null ? pp.arn()
				    	    : ARNHelper.serviceCatalogProvisionedProductArn(region, accountId, ppId);

				    Map<String, Object> attributes = new HashMap<>();
				    attributes.put("provisionedProductId",   ppId);
				    attributes.put("provisionedProductArn",  ppArn);
				    attributes.put("provisionedProductName", pp.name());
				    attributes.put("productId",              pp.productId());
				    attributes.put("provisioningArtifactId", pp.provisioningArtifactId());
				    attributes.put("status",
				        pp.status() != null ? pp.status().toString() : null);
				    attributes.put("createdTime",
				        pp.createdTime() != null ? pp.createdTime().toString() : null);
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
			log.error("ServiceCatalogProvisionedProductCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Service Catalog provisioned products", e);
		}
		catch (Exception e)
		{
			log.error("ServiceCatalogProvisionedProductCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Service Catalog provisioned products", e);
		}
	}
}
