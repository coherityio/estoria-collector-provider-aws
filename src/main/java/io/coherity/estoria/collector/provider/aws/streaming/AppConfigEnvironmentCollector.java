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
import software.amazon.awssdk.services.appconfig.model.Application;
import software.amazon.awssdk.services.appconfig.model.Environment;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsRequest;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsResponse;
import software.amazon.awssdk.services.appconfig.model.ListEnvironmentsRequest;
import software.amazon.awssdk.services.appconfig.model.ListEnvironmentsResponse;

/**
 * Collects AppConfig environments across all applications via the AppConfig
 * ListEnvironments API.
 */
@Slf4j
public class AppConfigEnvironmentCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppConfigEnvironment";

    private AppConfigClient appConfigClient;

    public AppConfigEnvironmentCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("appconfig", "configuration", "environment", "aws")).build());
        log.debug("AppConfigEnvironmentCollector created");
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
        log.debug("AppConfigEnvironmentCollector.collectEntities called");

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
                String envCursor = null;
                do
                {
                    ListEnvironmentsRequest.Builder envReq = ListEnvironmentsRequest.builder()
                        .applicationId(applicationId)
                        .maxResults(50);
                    if (envCursor != null) envReq.nextToken(envCursor);

                    ListEnvironmentsResponse envRes = this.appConfigClient.listEnvironments(envReq.build());
                    List<Environment> environments = envRes.items();

                    if (environments != null)
                    {
                        for (Environment env : environments)
                        {
                            if (env == null) continue;

                            String envId   = env.id();
                            String envName = env.name();
                            String envArn  = "arn:aws:appconfig:" + region + ":" + accountId
                                + ":application/" + applicationId + "/environment/" + envId;

                            Map<String, Object> attributes = new HashMap<>();
                            attributes.put("environmentId",   envId);
                            attributes.put("environmentName", envName);
                            attributes.put("environmentArn",  envArn);
                            attributes.put("applicationId",   applicationId);
                            attributes.put("description",     env.description());
                            attributes.put("state",           env.stateAsString());
                            attributes.put("accountId",       accountId);
                            attributes.put("region",          region);
                            attributes.put("monitorCount",
                                env.monitors() != null ? env.monitors().size() : 0);

                            CloudEntity entity = CloudEntity.builder()
                                .entityIdentifier(EntityIdentifier.builder()
                                    .id(envArn)
                                    .qualifiedResourceName(envArn)
                                    .build())
                                .entityType(ENTITY_TYPE)
                                .name(envName)
                                .collectorContext(collectorContext)
                                .attributes(attributes)
                                .rawPayload(env)
                                .collectedAt(now)
                                .build();

                            entities.add(entity);
                        }
                    }

                    envCursor = envRes.nextToken();
                }
                while (envCursor != null && !envCursor.isBlank());
            }

            log.debug("AppConfigEnvironmentCollector collected {} environments across {} applications",
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
            log.error("AppConfigEnvironmentCollector AppConfig error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect AppConfig environments", e);
        }
        catch (Exception e)
        {
            log.error("AppConfigEnvironmentCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting AppConfig environments", e);
        }
    }
}
