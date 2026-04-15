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
import software.amazon.awssdk.services.licensemanager.LicenseManagerClient;
import software.amazon.awssdk.services.licensemanager.model.LicenseConfiguration;
import software.amazon.awssdk.services.licensemanager.model.LicenseManagerException;
import software.amazon.awssdk.services.licensemanager.model.ListLicenseConfigurationsRequest;
import software.amazon.awssdk.services.licensemanager.model.ListLicenseConfigurationsResponse;

/**
 * Collects License Manager license configurations via the ListLicenseConfigurations API.
 */
@Slf4j
public class LicenseManagerLicenseConfigurationCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "LicenseManagerLicenseConfiguration";

	private LicenseManagerClient licenseManagerClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of())
			.tags(Set.of("governance", "license-manager", "compliance", "aws"))
			.build();

	public LicenseManagerLicenseConfigurationCollector()
	{
		log.debug("LicenseManagerLicenseConfigurationCollector created");
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
		log.debug("LicenseManagerLicenseConfigurationCollector.collectEntities called");

		if (this.licenseManagerClient == null)
		{
			this.licenseManagerClient = AwsClientFactory.getInstance().getLicenseManagerClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListLicenseConfigurationsRequest.Builder requestBuilder = ListLicenseConfigurationsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.maxResults(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("LicenseManagerLicenseConfigurationCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListLicenseConfigurationsResponse response =
				this.licenseManagerClient.listLicenseConfigurations(requestBuilder.build());
			List<LicenseConfiguration> configs   = response.licenseConfigurations();
			String                     nextToken = response.nextToken();

			log.debug("LicenseManagerLicenseConfigurationCollector received {} configs, nextToken={}",
				configs != null ? configs.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (configs != null)
			{
				for (LicenseConfiguration config : configs)
				{
					if (config == null) continue;

					String configArn  = config.licenseConfigurationArn();
					String configName = config.name();

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("licenseConfigurationArn",    configArn);
					attributes.put("licenseConfigurationId",     config.licenseConfigurationId());
					attributes.put("name",                       configName);
					attributes.put("description",                config.description());
					attributes.put("licenseCountingType",
						config.licenseCountingType() != null ? config.licenseCountingType().toString() : null);
					attributes.put("licenseCount",               config.licenseCount());
					attributes.put("licenseCountHardLimit",      config.licenseCountHardLimit());
					attributes.put("consumedLicenses",           config.consumedLicenses());
					attributes.put("status",                     config.status());
					attributes.put("ownerAccountId",             config.ownerAccountId());
					attributes.put("licenseRules",               config.licenseRules());
					attributes.put("disassociateWhenNotFound",   config.disassociateWhenNotFound());
					attributes.put("accountId",                  accountId);
					attributes.put("region",                     region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(configArn)
							.qualifiedResourceName(configArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(configName != null ? configName : configArn)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(config)
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
		catch (LicenseManagerException e)
		{
			log.error("LicenseManagerLicenseConfigurationCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect License Manager configurations", e);
		}
		catch (Exception e)
		{
			log.error("LicenseManagerLicenseConfigurationCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting License Manager configurations", e);
		}
	}
}
