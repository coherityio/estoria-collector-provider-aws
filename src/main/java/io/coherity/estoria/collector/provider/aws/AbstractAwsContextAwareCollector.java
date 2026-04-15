package io.coherity.estoria.collector.provider.aws;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAwsContextAwareCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	private final CollectorInfo collectorInfo;

	protected AbstractAwsContextAwareCollector(CollectorInfo collectorInfo)
	{
		this.collectorInfo = Validate.notNull(collectorInfo, "required: collectorInfo");
	}

	@FunctionalInterface
	protected interface CollectorInfoBuildStep
	{
		CollectorInfo build();
	}

	protected static CollectorInfoBuildStep awsCollectorInfoBuilder(String entityType, Set<String> requiredEntityTypes,
			Set<String> tags)
	{
		Validate.notBlank(entityType, "required: entityType");

		return () -> CollectorInfo.builder().providerId(PROVIDER_ID).entityType(entityType)
				.requiredEntityTypes(requiredEntityTypes == null ? Set.of() : requiredEntityTypes)
				.tags(tags == null ? Set.of() : tags)
				.supportedContextAttributes(CollectorContextAttributeKey.keysLowercase()).build();
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		return this.collectorInfo;
	}

	@Override
	public final CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext,
			CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		Validate.notNull(providerContext, "required: providerContext");
		Validate.notNull(collectorContext, "required: collectorContext");
		Validate.notNull(collectorRequestParams, "required: collectorRequestParams");

		AccountScope requiredAccountScope = this.getRequiredAccountScope();
		Validate.notNull(requiredAccountScope, "required: requiredAccountScope");

		AwsSessionContext awsSessionContext;
		try
		{
			awsSessionContext = AwsSessionContextResolver.getInstance().resolveSessionContext(providerContext);
		} catch (Exception e)
		{
			throw new CollectorException("Failed to resolve AWS session context", e);
		}

		if (awsSessionContext == null)
		{
			throw new CollectorException("Resolved AWS session context was null");
		}

		AccountScope actualAccountScope = awsSessionContext.getAccountScope();
		if (actualAccountScope == null)
		{
			throw new CollectorException("Resolved AWS account scope was null");
		}

		if (!actualAccountScope.satisfies(requiredAccountScope))
		{
//        	CollectorInfo collectorInfo = this.getCollectorInfo();	
//        	log.warn(
//                    "Collector: " + collectorInfo.getEntityType() + " requires account scope "
//                            + requiredAccountScope
//                            + " but current AWS session context resolved to "
//                            + actualAccountScope
//                            + " (profile="
//                            + awsSessionContext.getProfile()
//                            + ", currentAccountId="
//                            + awsSessionContext.getCurrentAccountId()
//                            + ", managementAccountId="
//                            + awsSessionContext.getManagementAccountId()
//                            + ")");
//        	return null;

			throw new CollectorException("Collector requires account scope " + requiredAccountScope
					+ " but current AWS session context resolved to " + actualAccountScope + " (profile="
					+ awsSessionContext.getProfile() + ", currentAccountId=" + awsSessionContext.getCurrentAccountId()
					+ ", managementAccountId=" + awsSessionContext.getManagementAccountId() + ")");
		}

		// If the caller specified a containment scope filter, skip collectors that
		// don't fall within it.
		ContainmentScope requestedContainmentScope = AwsSessionContextResolver.resolveContainmentScope(collectorContext);
		if (requestedContainmentScope != null && !requestedContainmentScope.includes(this.getEntityContainmentScope()))
		{
			log.debug("Skipping collector {}: containment scope {} is not included by requested {}",
					this.getClass().getSimpleName(), this.getEntityContainmentScope(), requestedContainmentScope);
			return emptyCollectorCursor();
		}

		EntityCategory requestedEntityCategory = AwsSessionContextResolver.resolveEntityCategory(collectorContext);
		if (requestedEntityCategory != null)
		{
			if (requestedEntityCategory != this.getEntityCategory())
			{
				log.debug("Skipping collector {}: entity category {} does not match requested {}",
						this.getClass().getSimpleName(), this.getEntityCategory(), requestedEntityCategory);
				return emptyCollectorCursor();
			}
		}
		else
		{
			boolean includeReference = AwsSessionContextResolver.resolveIncludeReferenceCategory(collectorContext);
			if (this.getEntityCategory() == EntityCategory.REFERENCE && !includeReference)
			{
				log.debug("Skipping collector {}: reference collector requires include-reference=true",
						this.getClass().getSimpleName());
				return emptyCollectorCursor();
			}
		}

		return this.collectEntities(providerContext, awsSessionContext, collectorContext, collectorRequestParams);
	}

	private static CollectorCursor emptyCollectorCursor()
	{
		return new CollectorCursor()
		{
			@Override
			public List<CloudEntity> getEntities()
			{
				return List.of();
			}
			@Override
			public Optional<String> getNextCursorToken()
			{
				return Optional.empty();
			}
			@Override
			public CursorMetadata getMetadata()
			{
				return CursorMetadata.builder().build();
			}
		};
	}

	// ==================================================================
	// ABSTRACT METHODS
	// ==================================================================

	public abstract AccountScope getRequiredAccountScope();

	public abstract ContainmentScope getEntityContainmentScope();

	public abstract EntityCategory getEntityCategory();

	public abstract CollectorCursor collectEntities(ProviderContext providerContext,
			AwsSessionContext awsSessionContext, CollectorContext collectorContext,
			CollectorRequestParams collectorRequestParams) throws CollectorException;

}