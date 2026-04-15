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
import software.amazon.awssdk.services.kinesisanalyticsv2.KinesisAnalyticsV2Client;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.ApplicationSummary;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.KinesisAnalyticsV2Exception;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.ListApplicationsRequest;
import software.amazon.awssdk.services.kinesisanalyticsv2.model.ListApplicationsResponse;

/**
 * Collects Kinesis Data Analytics (v2) applications via the KinesisAnalyticsV2
 * ListApplications API.
 */
@Slf4j
public class KinesisAnalyticsAppCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "KinesisAnalyticsApp";

    private KinesisAnalyticsV2Client analyticsClient;

    public KinesisAnalyticsAppCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("streaming", "kinesis", "analytics", "aws")).build());
        log.debug("KinesisAnalyticsAppCollector created");
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
        log.debug("KinesisAnalyticsAppCollector.collectEntities called");

        if (this.analyticsClient == null)
        {
            this.analyticsClient = AwsClientFactory.getInstance().getKinesisAnalyticsV2Client(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListApplicationsRequest.Builder requestBuilder = ListApplicationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("KinesisAnalyticsAppCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListApplicationsResponse response = this.analyticsClient.listApplications(requestBuilder.build());
            List<ApplicationSummary> apps     = response.applicationSummaries();
            String nextToken                  = response.nextToken();

            log.debug("KinesisAnalyticsAppCollector received {} applications, nextToken={}",
                apps != null ? apps.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (apps != null)
            {
                for (ApplicationSummary app : apps)
                {
                    if (app == null) continue;

                    String appArn  = app.applicationARN();
                    String appName = app.applicationName();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("applicationName",    appName);
                    attributes.put("applicationArn",     appArn);
                    attributes.put("accountId",          accountId);
                    attributes.put("region",             region);
                    attributes.put("applicationStatus",  app.applicationStatusAsString());
                    attributes.put("runtimeEnvironment", app.runtimeEnvironmentAsString());
                    attributes.put("applicationVersionId", app.applicationVersionId());

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
        catch (KinesisAnalyticsV2Exception e)
        {
            log.error("KinesisAnalyticsAppCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Kinesis Analytics applications", e);
        }
        catch (Exception e)
        {
            log.error("KinesisAnalyticsAppCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Kinesis Analytics applications", e);
        }
    }
}
