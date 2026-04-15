package io.coherity.estoria.collector.provider.aws.streaming;

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
import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.appconfig.model.AppConfigException;
import software.amazon.awssdk.services.appconfig.model.ConfigurationProfileSummary;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsRequest;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsResponse;
import software.amazon.awssdk.services.appconfig.model.ListConfigurationProfilesRequest;
import software.amazon.awssdk.services.appconfig.model.ListConfigurationProfilesResponse;

/**
 * Collects AppConfig configuration profiles across all applications via the AppConfig
 * ListConfigurationProfiles API.
 */
@Slf4j
public class AppConfigConfigurationProfileCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppConfigConfigurationProfile";

    private AppConfigClient appConfigClient;

    public AppConfigConfigurationProfileCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("appconfig", "configuration", "profile", "aws")).build());
        log.debug("AppConfigConfigurationProfileCollector created");
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
        log.debug("AppConfigConfigurationProfileCollector.collectEntities called");

        if (this.appConfigClient == null)
        {
            this.appConfigClient = AwsClientFactory.getInstance().getAppConfigClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            // Enumerate all applications first
            List<String> applicationIds = new ArrayList<>();
            String appCursor = null;
            do
            {
                ListApplicationsRequest.Builder appReq = ListApplicationsRequest.builder().maxResults(50);
                if (appCursor != null) appReq.nextToken(appCursor);
                ListApplicationsResponse appRes = this.appConfigClient.listApplications(appReq.build());
                if (appRes.items() != null)
                {
                    appRes.items().forEach(a -> { if (a.id() != null) applicationIds.add(a.id()); });
                }
                appCursor = appRes.nextToken();
            }
            while (appCursor != null && !appCursor.isBlank());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (String applicationId : applicationIds)
            {
                String profileCursor = null;
                do
                {
                    ListConfigurationProfilesRequest.Builder profileReq = ListConfigurationProfilesRequest.builder()
                        .applicationId(applicationId)
                        .maxResults(50);
                    if (profileCursor != null) profileReq.nextToken(profileCursor);

                    ListConfigurationProfilesResponse profileRes =
                        this.appConfigClient.listConfigurationProfiles(profileReq.build());
                    List<ConfigurationProfileSummary> profiles = profileRes.items();

                    if (profiles != null)
                    {
                        for (ConfigurationProfileSummary profile : profiles)
                        {
                            if (profile == null) continue;

                            String profileId   = profile.id();
                            String profileName = profile.name();
                            String profileArn  = "arn:aws:appconfig:" + region + ":" + accountId
                                + ":application/" + applicationId + "/configurationprofile/" + profileId;

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("configurationProfileId",   profileId);
                            attributes.put("configurationProfileName", profileName);
                            attributes.put("configurationProfileArn",  profileArn);
                            attributes.put("applicationId",            applicationId);
                            //attributes.put("description",              profile.description());
                            attributes.put("locationUri",              profile.locationUri());
                            //attributes.put("retrievalRoleArn",         profile.retrievalRoleArn());
                            attributes.put("type",                     profile.type());
                            attributes.put("validatorTypeCount",
                                profile.validatorTypes() != null ? profile.validatorTypes().size() : 0);
                            attributes.put("accountId",                accountId);
                            attributes.put("region",                   region);

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(profileArn)
                                    .qualifiedResourceName(profileArn)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(profileName)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(profile)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }

                    profileCursor = profileRes.nextToken();
                }
                while (profileCursor != null && !profileCursor.isBlank());
            }

            log.debug("AppConfigConfigurationProfileCollector collected {} profiles across {} applications",
                entities.size(), applicationIds.size());

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (AppConfigException e)
        {
            log.error("AppConfigConfigurationProfileCollector AppConfig error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect AppConfig configuration profiles", e);
        }
        catch (Exception e)
        {
            log.error("AppConfigConfigurationProfileCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting AppConfig configuration profiles", e);
        }
    }
}
