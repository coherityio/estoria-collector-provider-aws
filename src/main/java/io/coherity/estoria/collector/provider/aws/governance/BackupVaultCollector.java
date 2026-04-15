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
import software.amazon.awssdk.services.backup.model.BackupVaultListMember;
import software.amazon.awssdk.services.backup.model.ListBackupVaultsRequest;
import software.amazon.awssdk.services.backup.model.ListBackupVaultsResponse;

/**
 * Collects AWS Backup vaults via the ListBackupVaults API.
 */
@Slf4j
public class BackupVaultCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public  static final String ENTITY_TYPE = "BackupVault";

	private BackupClient backupClient;

	private final CollectorInfo collectorInfo =
		CollectorInfo.builder()
			.providerId(PROVIDER_ID)
			.entityType(ENTITY_TYPE)
			.requiredEntityTypes(Set.of())
			.tags(Set.of("governance", "backup", "aws"))
			.build();

	public BackupVaultCollector()
	{
		log.debug("BackupVaultCollector created");
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
		log.debug("BackupVaultCollector.collectEntities called");

		if (this.backupClient == null)
		{
			this.backupClient = AwsClientFactory.getInstance().getBackupClient(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListBackupVaultsRequest.Builder requestBuilder = ListBackupVaultsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.maxResults(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("BackupVaultCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListBackupVaultsResponse response = this.backupClient.listBackupVaults(requestBuilder.build());
			List<BackupVaultListMember> vaults    = response.backupVaultList();
			String                      nextToken = response.nextToken();

			log.debug("BackupVaultCollector received {} vaults, nextToken={}",
				vaults != null ? vaults.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (vaults != null)
			{
				for (BackupVaultListMember vault : vaults)
				{
					if (vault == null) continue;

					String vaultName = vault.backupVaultName();
					String vaultArn  = vault.backupVaultArn() != null ? vault.backupVaultArn()
						: "arn:aws:backup:" + region + ":" + accountId + ":backup-vault:" + vaultName;

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("vaultName",            vaultName);
					attributes.put("vaultArn",             vaultArn);
					attributes.put("encryptionKeyArn",     vault.encryptionKeyArn());
					attributes.put("creationDate",
						vault.creationDate() != null ? vault.creationDate().toString() : null);
					attributes.put("creatorRequestId",     vault.creatorRequestId());
					attributes.put("numberOfRecoveryPoints", vault.numberOfRecoveryPoints());
					attributes.put("locked",               vault.locked());
					attributes.put("minRetentionDays",     vault.minRetentionDays());
					attributes.put("maxRetentionDays",     vault.maxRetentionDays());
					attributes.put("lockDate",
						vault.lockDate() != null ? vault.lockDate().toString() : null);
					attributes.put("accountId",            accountId);
					attributes.put("region",               region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(vaultArn)
							.qualifiedResourceName(vaultArn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(vaultName)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(vault)
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
			log.error("BackupVaultCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Backup vaults", e);
		}
		catch (Exception e)
		{
			log.error("BackupVaultCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting Backup vaults", e);
		}
	}
}
