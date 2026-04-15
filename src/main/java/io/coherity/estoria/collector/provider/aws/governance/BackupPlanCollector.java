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
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.backup.model.BackupException;
import software.amazon.awssdk.services.backup.model.BackupPlansListMember;
import software.amazon.awssdk.services.backup.model.ListBackupPlansRequest;
import software.amazon.awssdk.services.backup.model.ListBackupPlansResponse;

/**
 * Collects AWS Backup plans via the ListBackupPlans API.
 */
@Slf4j
public class BackupPlanCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "BackupPlan";

	private BackupClient backupClient;

	public BackupPlanCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(BackupVaultCollector.ENTITY_TYPE), Set.of("governance", "backup", "aws")).build());
		log.debug("BackupPlanCollector created");
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
		log.debug("BackupPlanCollector.collectEntities called");

		if (this.backupClient == null)
		{
			this.backupClient = AwsClientFactory.getInstance().getBackupClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListBackupPlansRequest.Builder requestBuilder = ListBackupPlansRequest.builder()
				.includeDeleted(false);

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.maxResults(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("BackupPlanCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListBackupPlansResponse response = this.backupClient.listBackupPlans(requestBuilder.build());
			List<BackupPlansListMember> plans     = response.backupPlansList();
			String                      nextToken = response.nextToken();

			log.debug("BackupPlanCollector received {} plans, nextToken={}",
				plans != null ? plans.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (plans != null)
			{
				for (BackupPlansListMember plan : plans)
				{
					if (plan == null) continue;

					String planId  = plan.backupPlanId();
					String planArn = plan.backupPlanArn() != null ? plan.backupPlanArn()
						: "arn:aws:backup:" + region + ":" + accountId + ":backup-plan:" + planId;

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("backupPlanId",       planId);
					attributes.put("backupPlanArn",      planArn);
					attributes.put("backupPlanName",     plan.backupPlanName());
					attributes.put("versionId",          plan.versionId());
					attributes.put("creatorRequestId",   plan.creatorRequestId());
					attributes.put("creationDate",
						plan.creationDate() != null ? plan.creationDate().toString() : null);
					attributes.put("deletionDate",
						plan.deletionDate() != null ? plan.deletionDate().toString() : null);
					attributes.put("lastExecutionDate",
						plan.lastExecutionDate() != null ? plan.lastExecutionDate().toString() : null);
					attributes.put("advancedBackupSettings",
						plan.advancedBackupSettings() != null
							? plan.advancedBackupSettings().stream()
								.map(s -> Map.of(
									"resourceType", s.resourceType() != null ? s.resourceType() : "",
									"backupOptions", s.backupOptions()))
								.toList()
							: null);
					attributes.put("accountId",          accountId);
					attributes.put("region",             region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(planArn)
							.qualifiedResourceName(planArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(plan.backupPlanName() != null ? plan.backupPlanName() : planId)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(plan)
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
		catch (BackupException e)
		{
			log.error("BackupPlanCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Backup plans", e);
		}
		catch (Exception e)
		{
			log.error("BackupPlanCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Backup plans", e);
		}
	}
}
