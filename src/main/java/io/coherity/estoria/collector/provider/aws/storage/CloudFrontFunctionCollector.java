package io.coherity.estoria.collector.provider.aws.storage;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudfront.model.CloudFrontException;
import software.amazon.awssdk.services.cloudfront.model.FunctionSummary;
import software.amazon.awssdk.services.cloudfront.model.ListFunctionsRequest;
import software.amazon.awssdk.services.cloudfront.model.ListFunctionsResponse;

/**
 * Collects CloudFront Functions via the CloudFront ListFunctions API.
 */
@Slf4j
public class CloudFrontFunctionCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "CloudFrontFunction";

    private CloudFrontClient cloudFrontClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("storage", "cdn", "cloudfront", "function", "aws"))
            .build();

    public CloudFrontFunctionCollector()
    {
        log.debug("CloudFrontFunctionCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("CloudFrontFunctionCollector.collect called");

        if (this.cloudFrontClient == null)
        {
            this.cloudFrontClient = AwsClientFactory.getInstance().getCloudFrontClient(providerContext);
        }

        try
        {
            ListFunctionsRequest.Builder requestBuilder = ListFunctionsRequest.builder()
                .stage("LIVE");

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxItems(String.valueOf(pageSize));
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CloudFrontFunctionCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            ListFunctionsResponse response = this.cloudFrontClient.listFunctions(requestBuilder.build());
            List<FunctionSummary> functions = response.functionList() != null
                ? response.functionList().items() : null;
            String nextMarker = response.functionList() != null
                ? response.functionList().nextMarker() : null;

            log.debug("CloudFrontFunctionCollector received {} functions, nextMarker={}",
                functions != null ? functions.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (functions != null)
            {
                for (FunctionSummary fn : functions)
                {
                    if (fn == null) continue;

                    String name = fn.name();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", name);
                    attributes.put("status", fn.status());
                    if (fn.functionMetadata() != null)
                    {
                        attributes.put("functionArn", fn.functionMetadata().functionARN());
                        attributes.put("stage", fn.functionMetadata().stageAsString());
                        attributes.put("runtime", fn.functionConfig().runtimeAsString());
                        attributes.put("createdTime", fn.functionMetadata().createdTime() != null
                            ? fn.functionMetadata().createdTime().toString() : null);
                        attributes.put("lastModifiedTime", fn.functionMetadata().lastModifiedTime() != null
                            ? fn.functionMetadata().lastModifiedTime().toString() : null);
                    }
                    if (fn.functionConfig() != null)
                    {
                        attributes.put("comment", fn.functionConfig().comment());
                        attributes.put("runtime", fn.functionConfig().runtimeAsString());
                    }

                    String arn = fn.functionMetadata() != null ? fn.functionMetadata().functionARN() : name;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(fn)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextMarker = nextMarker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextMarker).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (CloudFrontException e)
        {
            log.error("CloudFrontFunctionCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect CloudFront Functions", e);
        }
        catch (Exception e)
        {
            log.error("CloudFrontFunctionCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudFront Functions", e);
        }
    }
}
