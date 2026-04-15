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
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Crawler;
import software.amazon.awssdk.services.glue.model.GetCrawlersRequest;
import software.amazon.awssdk.services.glue.model.GetCrawlersResponse;
import software.amazon.awssdk.services.glue.model.GlueException;

/**
 * Collects Glue Crawlers via the Glue GetCrawlers API.
 */
@Slf4j
public class GlueCrawlerCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "GlueCrawler";

    private GlueClient glueClient;

    public GlueCrawlerCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("integration", "glue", "crawler", "aws")).build());
        log.debug("GlueCrawlerCollector created");
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
        log.debug("GlueCrawlerCollector.collectEntities called");

        if (this.glueClient == null)
        {
            this.glueClient = AwsClientFactory.getInstance().getGlueClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetCrawlersRequest.Builder requestBuilder = GetCrawlersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("GlueCrawlerCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            GetCrawlersResponse response = this.glueClient.getCrawlers(requestBuilder.build());
            List<Crawler> crawlers = response.crawlers();
            String        nextToken = response.nextToken();

            log.debug("GlueCrawlerCollector received {} crawlers, nextToken={}",
                crawlers != null ? crawlers.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (crawlers != null)
            {
                for (Crawler crawler : crawlers)
                {
                    if (crawler == null) continue;

                    String crawlerName = crawler.name();
                    // ARN: arn:aws:glue:<region>:<accountId>:crawler/<name>
                    String crawlerArn  = "arn:aws:glue:" + region + ":" + accountId
                        + ":crawler/" + crawlerName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("crawlerName",       crawlerName);
                    attributes.put("crawlerArn",        crawlerArn);
                    attributes.put("description",       crawler.description());
                    attributes.put("role",              crawler.role());
                    attributes.put("state",
                        crawler.state() != null ? crawler.state().toString() : null);
                    attributes.put("databaseName",      crawler.databaseName());
                    attributes.put("schedule",
                        crawler.schedule() != null ? crawler.schedule().scheduleExpression() : null);
                    attributes.put("creationTime",
                        crawler.creationTime() != null ? crawler.creationTime().toString() : null);
                    attributes.put("lastUpdated",
                        crawler.lastUpdated() != null ? crawler.lastUpdated().toString() : null);
                    attributes.put("lastCrawlStatus",
                        crawler.lastCrawl() != null && crawler.lastCrawl().status() != null
                            ? crawler.lastCrawl().status().toString() : null);
                    attributes.put("version",           crawler.version());
                    attributes.put("accountId",         accountId);
                    attributes.put("region",            region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(crawlerArn)
                            .qualifiedResourceName(crawlerArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(crawlerName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(crawler)
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
        catch (GlueException e)
        {
            log.error("GlueCrawlerCollector Glue error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Glue crawlers", e);
        }
        catch (Exception e)
        {
            log.error("GlueCrawlerCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Glue crawlers", e);
        }
    }
}
