package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.s3control.model.ListMultiRegionAccessPointsRequest;
import software.amazon.awssdk.services.s3control.model.ListMultiRegionAccessPointsResponse;
import software.amazon.awssdk.services.s3control.model.MultiRegionAccessPointReport;
import software.amazon.awssdk.services.s3control.model.RegionReport;
import software.amazon.awssdk.services.s3control.model.S3ControlException;

/**
 * Collects S3 Multi-Region Access Points (MRAPs) via the S3Control API.
 */
@Slf4j
public class S3MultiRegionAccessPointCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "S3MultiRegionAccessPoint";


    public S3MultiRegionAccessPointCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("storage", "s3", "mrap", "multi-region", "aws")).build());
        log.debug("S3MultiRegionAccessPointCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.AWS_GLOBAL; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("S3MultiRegionAccessPointCollector.collect called");

        S3ControlClient s3ControlClient = AwsClientFactory.getInstance().getS3ControlClient(providerContext);

        String accountId = awsSessionContext.getCurrentAccountId();

        try
        {
            ListMultiRegionAccessPointsRequest.Builder requestBuilder =
                ListMultiRegionAccessPointsRequest.builder().accountId(accountId);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("S3MultiRegionAccessPointCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListMultiRegionAccessPointsResponse response =
                s3ControlClient.listMultiRegionAccessPoints(requestBuilder.build());
            List<MultiRegionAccessPointReport> reports = response.accessPoints();
            String nextToken = response.nextToken();

            log.debug("S3MultiRegionAccessPointCollector received {} MRAPs, nextToken={}",
                reports != null ? reports.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (reports != null)
            {
                for (MultiRegionAccessPointReport report : reports)
                {
                    if (report == null) continue;

                    String name = report.name();
                    String alias = report.alias();
                    String arn = ARNHelper.s3MultiRegionAccessPointArn(accountId, name);

                    List<String> regions = new ArrayList<>();
                    if (report.regions() != null)
                    {
                        for (RegionReport r : report.regions())
                        {
                            regions.add(r.bucket() + "@" + r.region());
                        }
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", name);
                    attributes.put("alias", alias);
                    attributes.put("arn", arn);
                    attributes.put("status", report.statusAsString());
                    attributes.put("createdAt", report.createdAt() != null ? report.createdAt().toString() : null);
                    attributes.put("regions", regions);
                    if (report.publicAccessBlock() != null)
                    {
                        attributes.put("blockPublicAcls", report.publicAccessBlock().blockPublicAcls());
                        attributes.put("ignorePublicAcls", report.publicAccessBlock().ignorePublicAcls());
                        attributes.put("blockPublicPolicy", report.publicAccessBlock().blockPublicPolicy());
                        attributes.put("restrictPublicBuckets", report.publicAccessBlock().restrictPublicBuckets());
                    }

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(report)
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
        catch (S3ControlException e)
        {
            log.error("S3MultiRegionAccessPointCollector S3Control error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect S3 Multi-Region Access Points", e);
        }
        catch (Exception e)
        {
            log.error("S3MultiRegionAccessPointCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting S3 Multi-Region Access Points", e);
        }
    }

}
