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
import software.amazon.awssdk.services.backup.model.ListBackupVaultsRequest;
import software.amazon.awssdk.services.backup.model.ListBackupVaultsResponse;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByBackupVaultRequest;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByBackupVaultResponse;
import software.amazon.awssdk.services.backup.model.RecoveryPointByBackupVault;

/**
 * Collects AWS Backup recovery points by enumerating all vaults then
 * calling ListRecoveryPointsByBackupVault per vault.
 */
@Slf4j
public class BackupRecoveryPointCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "BackupRecoveryPoint";

	private BackupClient backupClient;

	public BackupRecoveryPointCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(BackupVaultCollector.ENTITY_TYPE), Set.of("governance", "backup", "aws")).build());
		log.debug("BackupRecoveryPointCollector created");
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
		log.debug("BackupRecoveryPointCollector.collectEntities called");

		if (this.backupClient == null)
		{
			this.backupClient = AwsClientFactory.getInstance().getBackupClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			// Step 1: enumerate all vault names
			List<String> vaultNames = new ArrayList<>();
			String vaultToken = null;
			do
			{
				ListBackupVaultsRequest.Builder vaultReq = ListBackupVaultsRequest.builder();
				if (vaultToken != null) vaultReq.nextToken(vaultToken);
				ListBackupVaultsResponse vaultResp = this.backupClient.listBackupVaults(vaultReq.build());
				if (vaultResp.backupVaultList() != null)
				{
					vaultResp.backupVaultList().forEach(v -> {
						if (v != null && v.backupVaultName() != null) vaultNames.add(v.backupVaultName());
					});
				}
				vaultToken = vaultResp.nextToken();
			}
			while (vaultToken != null && !vaultToken.isBlank());

			log.debug("BackupRecoveryPointCollector found {} vaults to inspect", vaultNames.size());

			// Step 2: list recovery points per vault
			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			int pageSize = collectorRequestParams.getPageSize();

			for (String vaultName : vaultNames)
			{
				String rpToken = null;
				do
				{
					ListRecoveryPointsByBackupVaultRequest.Builder rpReq =
						ListRecoveryPointsByBackupVaultRequest.builder().backupVaultName(vaultName);
					if (pageSize > 0) rpReq.maxResults(pageSize);
					if (rpToken != null) rpReq.nextToken(rpToken);

					ListRecoveryPointsByBackupVaultResponse rpResp =
						this.backupClient.listRecoveryPointsByBackupVault(rpReq.build());
					rpToken = rpResp.nextToken();

					List<RecoveryPointByBackupVault> points = rpResp.recoveryPoints();
					if (points != null)
					{
						for (RecoveryPointByBackupVault rp : points)
						{
							if (rp == null) continue;

							String rpArn = rp.recoveryPointArn();

							Map<String, Object> attributes = new HashMap<>();
							attributes.put("recoveryPointArn",    rpArn);
							attributes.put("backupVaultName",     rp.backupVaultName());
							attributes.put("backupVaultArn",      rp.backupVaultArn());
							attributes.put("resourceArn",         rp.resourceArn());
							attributes.put("resourceType",        rp.resourceType());
							attributes.put("status",
								rp.status() != null ? rp.status().toString() : null);
							attributes.put("statusMessage",       rp.statusMessage());
							attributes.put("creationDate",
								rp.creationDate() != null ? rp.creationDate().toString() : null);
							attributes.put("completionDate",
								rp.completionDate() != null ? rp.completionDate().toString() : null);
							attributes.put("backupSizeInBytes",   rp.backupSizeInBytes());
							attributes.put("iamRoleArn",          rp.iamRoleArn());
							attributes.put("backupPlanArn",
								    rp.createdBy() != null ? rp.createdBy().backupPlanArn() : null);
							attributes.put("backupPlanId",
							    rp.createdBy() != null ? rp.createdBy().backupPlanId() : null);
							attributes.put("backupPlanVersion",
							    rp.createdBy() != null ? rp.createdBy().backupPlanVersion() : null);
							attributes.put("backupRuleId",
							    rp.createdBy() != null ? rp.createdBy().backupRuleId() : null);							
							attributes.put("isEncrypted",         rp.isEncrypted());
							attributes.put("encryptionKeyArn",    rp.encryptionKeyArn());
							attributes.put("accountId",           accountId);
							attributes.put("region",              region);

							String rpName = rpArn != null && rpArn.contains(":")
								? rpArn.substring(rpArn.lastIndexOf(':') + 1)
								: rpArn;

							CloudEntity entity = CloudEntity.builder()
								.entityIdentifier(EntityIdentifier.builder()
									.id(rpArn)
									.qualifiedResourceName(rpArn)
									.build())
								.entityType(ENTITY_TYPE)
								.name(rpName)
								.collectorContext(collectorContext)
								.attributes(attributes)
								.rawPayload(rp)
								.collectedAt(now)
								.build();

							entities.add(entity);
						}
					}
				}
				while (rpToken != null && !rpToken.isBlank());
			}

			log.debug("BackupRecoveryPointCollector collected {} recovery points across {} vaults",
				entities.size(), vaultNames.size());

			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());
			metadataValues.put("vaultCount", vaultNames.size());

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
			log.error("BackupRecoveryPointCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Backup recovery points", e);
		}
		catch (Exception e)
		{
			log.error("BackupRecoveryPointCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Backup recovery points", e);
		}
	}
}
