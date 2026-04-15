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
import software.amazon.awssdk.services.glue.model.GetJobsRequest;
import software.amazon.awssdk.services.glue.model.GetJobsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Job;

/**
 * Collects Glue ETL jobs via the Glue GetJobs API.
 */
@Slf4j
public class GlueJobCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "GlueJob";

    private GlueClient glueClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("integration", "glue", "etl", "aws"))
            .build();

    public GlueJobCollector()
    {
        log.debug("GlueJobCollector created");
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
        log.debug("GlueJobCollector.collectEntities called");

        if (this.glueClient == null)
        {
            this.glueClient = AwsClientFactory.getInstance().getGlueClient(providerContext);
        }

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetJobsRequest.Builder requestBuilder = GetJobsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("GlueJobCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            GetJobsResponse response = this.glueClient.getJobs(requestBuilder.build());
            List<Job> jobs    = response.jobs();
            String    nextToken = response.nextToken();

            log.debug("GlueJobCollector received {} jobs, nextToken={}",
                jobs != null ? jobs.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (jobs != null)
            {
                for (Job job : jobs)
                {
                    if (job == null) continue;

                    String jobName = job.name();
                    // ARN: arn:aws:glue:<region>:<accountId>:job/<name>
                    String jobArn  = "arn:aws:glue:" + region + ":" + accountId + ":job/" + jobName;

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("jobName",           jobName);
                    attributes.put("jobArn",            jobArn);
                    attributes.put("description",       job.description());
                    attributes.put("role",              job.role());
                    attributes.put("glueVersion",       job.glueVersion());
                    attributes.put("maxRetries",        job.maxRetries());
                    attributes.put("timeout",           job.timeout());
                    attributes.put("maxCapacity",       job.maxCapacity());
                    attributes.put("numberOfWorkers",   job.numberOfWorkers());
                    attributes.put("workerType",
                        job.workerType() != null ? job.workerType().toString() : null);
                    attributes.put("createdOn",
                        job.createdOn() != null ? job.createdOn().toString() : null);
                    attributes.put("lastModifiedOn",
                        job.lastModifiedOn() != null ? job.lastModifiedOn().toString() : null);
                    attributes.put("defaultArguments", job.defaultArguments());
                    attributes.put("connections",
                        job.connections() != null ? job.connections().connections() : null);
                    attributes.put("accountId",         accountId);
                    attributes.put("region",            region);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(jobArn)
                            .qualifiedResourceName(jobArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(jobName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(job)
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
            log.error("GlueJobCollector Glue error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Glue jobs", e);
        }
        catch (Exception e)
        {
            log.error("GlueJobCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Glue jobs", e);
        }
    }
}
