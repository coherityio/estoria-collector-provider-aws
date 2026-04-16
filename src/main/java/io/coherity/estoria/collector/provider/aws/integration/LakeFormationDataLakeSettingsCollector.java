package io.coherity.estoria.collector.provider.aws.integration;

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
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.DataLakeSettings;
import software.amazon.awssdk.services.lakeformation.model.GetDataLakeSettingsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetDataLakeSettingsResponse;
import software.amazon.awssdk.services.lakeformation.model.LakeFormationException;

/**
 * Collects Lake Formation data lake settings via the GetDataLakeSettings API.
 * Emits a single settings entity per account/region.
 */
@Slf4j
public class LakeFormationDataLakeSettingsCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "LakeFormationDataLakeSettings";


    public LakeFormationDataLakeSettingsCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "lake-formation", "governance", "aws")).build());
        log.debug("LakeFormationDataLakeSettingsCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.REFERENCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("LakeFormationDataLakeSettingsCollector.collectEntities called");

        LakeFormationClient lakeFormationClient = AwsClientFactory.getInstance().getLakeFormationClient(providerContext);

        List<CloudEntity> entities = new ArrayList<>();
        Instant now = Instant.now();

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetDataLakeSettingsResponse response =
                lakeFormationClient.getDataLakeSettings(
                    GetDataLakeSettingsRequest.builder().catalogId(accountId).build());

            DataLakeSettings settings = response.dataLakeSettings();

            // Synthetic ARN for settings
            String settingsId = "arn:aws:lakeformation:" + region + ":" + accountId + ":settings";

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("settingsId",                settingsId);
            attributes.put("accountId",                 accountId);
            attributes.put("region",                    region);
            attributes.put("dataLakeAdmins",
                settings.dataLakeAdmins() != null
                    ? settings.dataLakeAdmins().stream()
                        .map(p -> p.dataLakePrincipalIdentifier()).toList()
                    : null);
            attributes.put("readOnlyAdmins",
                settings.readOnlyAdmins() != null
                    ? settings.readOnlyAdmins().stream()
                        .map(p -> p.dataLakePrincipalIdentifier()).toList()
                    : null);
            attributes.put("createDatabaseDefaultPermissions",
                settings.createDatabaseDefaultPermissions() != null
                    ? settings.createDatabaseDefaultPermissions().stream()
                        .map(p -> Map.of(
                            "principal", p.principal() != null ? p.principal().dataLakePrincipalIdentifier() : null,
                            "permissions", p.permissionsAsStrings()))
                        .toList()
                    : null);
            attributes.put("createTableDefaultPermissions",
                settings.createTableDefaultPermissions() != null
                    ? settings.createTableDefaultPermissions().stream()
                        .map(p -> Map.of(
                            "principal", p.principal() != null ? p.principal().dataLakePrincipalIdentifier() : null,
                            "permissions", p.permissionsAsStrings()))
                        .toList()
                    : null);
            attributes.put("trustedResourceOwners",          settings.trustedResourceOwners());
            attributes.put("allowExternalDataFiltering",     settings.allowExternalDataFiltering());
            attributes.put("allowFullTableExternalDataAccess", settings.allowFullTableExternalDataAccess());

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id(settingsId)
                    .qualifiedResourceName(settingsId)
                    .build())
                .entityType(ENTITY_TYPE)
                .name("LakeFormationSettings-" + accountId + "-" + region)
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(response)
                .collectedAt(now)
                .build();

            entities.add(entity);
            log.debug("LakeFormationDataLakeSettingsCollector collected settings for account {}", accountId);
        }
        catch (LakeFormationException e)
        {
            log.error("LakeFormationDataLakeSettingsCollector Lake Formation error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Lake Formation data lake settings", e);
        }
        catch (Exception e)
        {
            log.error("LakeFormationDataLakeSettingsCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Lake Formation data lake settings", e);
        }

        Map<String, Object> metadataValues = new HashMap<>();
        metadataValues.put("count", entities.size());

        List<CloudEntity> finalEntities = entities;
        return new CollectorCursor()
        {
            @Override public List<CloudEntity> getEntities() { return finalEntities; }
            @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
            @Override public CursorMetadata getMetadata()
            {
                return CursorMetadata.builder().values(metadataValues).build();
            }
        };
    }
}
