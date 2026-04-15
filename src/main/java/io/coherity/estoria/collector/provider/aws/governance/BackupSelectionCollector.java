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
import software.amazon.awssdk.services.backup.model.BackupSelectionsListMember;
import software.amazon.awssdk.services.backup.model.ListBackupPlansRequest;
import software.amazon.awssdk.services.backup.model.ListBackupPlansResponse;
import software.amazon.awssdk.services.backup.model.ListBackupSelectionsRequest;
import software.amazon.awssdk.services.backup.model.ListBackupSelectionsResponse;

/**
 * Collects AWS Backup selections by enumerating all backup plans then
 * calling ListBackupSelections per plan.
 */
@Slf4j
public class BackupSelectionCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "BackupSelection";

	private BackupClient backupClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of(BackupPlanCollector.ENTITY_TYPE))
			.tags(Set.of("governance", "backup", "aws"))
			.build();

	public BackupSelectionCollector()
	{
		log.debug("BackupSelectionCollector created");
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
		log.debug("BackupSelectionCollector.collectEntities called");

		if (this.backupClient == null)
		{
			this.backupClient = AwsClientFactory.getInstance().getBackupClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			// Step 1: enumerate all backup plan IDs
			List<String> planIds   = new ArrayList<>();
			String       planToken = null;
			do
			{
				ListBackupPlansRequest.Builder planReq = ListBackupPlansRequest.builder().includeDeleted(false);
				if (planToken != null) planReq.nextToken(planToken);
				ListBackupPlansResponse planResp = this.backupClient.listBackupPlans(planReq.build());
				if (planResp.backupPlansList() != null)
				{
					for (BackupPlansListMember plan : planResp.backupPlansList())
					{
						if (plan != null && plan.backupPlanId() != null) planIds.add(plan.backupPlanId());
					}
				}
				planToken = planResp.nextToken();
			}
			while (planToken != null && !planToken.isBlank());

			log.debug("BackupSelectionCollector found {} backup plans to inspect", planIds.size());

			// Step 2: list selections per plan
			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			for (String planId : planIds)
			{
				String selToken = null;
				do
				{
					ListBackupSelectionsRequest.Builder selReq = ListBackupSelectionsRequest.builder()
						.backupPlanId(planId);
					if (selToken != null) selReq.nextToken(selToken);

					ListBackupSelectionsResponse selResp = this.backupClient.listBackupSelections(selReq.build());
					selToken = selResp.nextToken();

					List<BackupSelectionsListMember> selections = selResp.backupSelectionsList();
					if (selections != null)
					{
						for (BackupSelectionsListMember sel : selections)
						{
							if (sel == null) continue;

							String selectionId = sel.selectionId();
							// Synthetic ARN — backup selections don't have native ARNs
							String selectionArn = "arn:aws:backup:" + region + ":" + accountId
								+ ":backup-plan:" + planId + "/selection/" + selectionId;

							Map<String, Object> attributes = new HashMap<>();
							attributes.put("selectionId",       selectionId);
							attributes.put("selectionArn",      selectionArn);
							attributes.put("selectionName",     sel.selectionName());
							attributes.put("backupPlanId",      planId);
							attributes.put("iamRoleArn",        sel.iamRoleArn());
							attributes.put("creatorRequestId",  sel.creatorRequestId());
							attributes.put("creationDate",
								sel.creationDate() != null ? sel.creationDate().toString() : null);
							attributes.put("accountId",         accountId);
							attributes.put("region",            region);

							CloudEntity entity = CloudEntity.builder()
								.entityIdentifier(EntityIdentifier.builder()
									.id(selectionArn)
									.qualifiedResourceName(selectionArn)
									.build())
								.entityType(ENTITY_TYPE)
								.name(sel.selectionName() != null ? sel.selectionName() : selectionId)
								.collectorContext(collectorContext)
								.attributes(attributes)
								.rawPayload(sel)
								.collectedAt(now)
								.build();

							entities.add(entity);
						}
					}
				}
				while (selToken != null && !selToken.isBlank());
			}

			log.debug("BackupSelectionCollector collected {} selections across {} plans",
				entities.size(), planIds.size());

			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());
			metadataValues.put("planCount", planIds.size());

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (BackupException e)
		{
			log.error("BackupSelectionCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Backup selections", e);
		}
		catch (Exception e)
		{
			log.error("BackupSelectionCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Backup selections", e);
		}
	}
}
