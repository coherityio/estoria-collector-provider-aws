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
import software.amazon.awssdk.services.appconfig.model.ListApplicationsRequest;
import software.amazon.awssdk.services.appconfig.model.ListApplicationsResponse;

/**
 * Collects AppConfig applications via the AppConfig ListApplications API.
 */
@Slf4j
public class AppConfigApplicationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppConfigApplication";


    public AppConfigApplicationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("appconfig", "configuration", "aws")).build());
        log.debug("AppConfigApplicationCollector created");
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
        log.debug("AppConfigApplicationCollector.collectEntities called");

        AppConfigClient appConfigClient = AwsClientFactory.getInstance().getAppConfigClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListApplicationsRequest.Builder requestBuilder = ListApplicationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("AppConfigApplicationCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListApplicationsResponse response = appConfigClient.listApplications(requestBuilder.build());
            List<Application> apps   = response.items();
            String nextToken         = response.nextToken();

            log.debug("AppConfigApplicationCollector received {} applications, nextToken={}",
                apps != null ? apps.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (apps != null)
            {
                for (Application app : apps)
                {
                    if (app == null) continue;

                    String appId   = app.id();
                    String appName = app.name();
                    String appArn  = "arn:aws:appconfig:" + region + ":" + accountId + ":application/" + appId;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("applicationId",   appId);
                    attributes.put("applicationName", appName);
                    attributes.put("applicationArn",  appArn);
                    attributes.put("description",     app.description());
                    attributes.put("accountId",       accountId);
                    attributes.put("region",          region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(appArn)
                            .qualifiedResourceName(appArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(appName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(app)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (AppConfigException e)
        {
            log.error("AppConfigApplicationCollector AppConfig error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect AppConfig applications", e);
        }
        catch (Exception e)
        {
            log.error("AppConfigApplicationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting AppConfig applications", e);
        }
    }
}
