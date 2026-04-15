package io.coherity.estoria.collector.provider.aws.nosql;

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
import software.amazon.awssdk.services.opensearch.OpenSearchClient;
import software.amazon.awssdk.services.opensearch.model.ListVpcEndpointsRequest;
import software.amazon.awssdk.services.opensearch.model.ListVpcEndpointsResponse;
import software.amazon.awssdk.services.opensearch.model.OpenSearchException;
import software.amazon.awssdk.services.opensearch.model.VpcEndpointSummary;

/**
 * Collects OpenSearch VPC endpoints via the OpenSearch ListVpcEndpoints API.
 */
@Slf4j
public class OpenSearchVpcEndpointCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "OpenSearchVpcEndpoint";

    private OpenSearchClient openSearchClient;

    public OpenSearchVpcEndpointCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "nosql", "opensearch", "vpc", "aws")).build());
        log.debug("OpenSearchVpcEndpointCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.VPC; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("OpenSearchVpcEndpointCollector.collectEntities called");

        if (this.openSearchClient == null)
        {
            this.openSearchClient = AwsClientFactory.getInstance().getOpenSearchClient(providerContext);
        }

        try
        {
            ListVpcEndpointsRequest.Builder requestBuilder = ListVpcEndpointsRequest.builder();

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("OpenSearchVpcEndpointCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListVpcEndpointsResponse response = this.openSearchClient.listVpcEndpoints(requestBuilder.build());
            List<VpcEndpointSummary> endpoints = response.vpcEndpointSummaryList();
            String nextToken = response.nextToken();

            log.debug("OpenSearchVpcEndpointCollector received {} VPC endpoints, nextToken={}",
                endpoints != null ? endpoints.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (endpoints != null)
            {
                for (VpcEndpointSummary endpoint : endpoints)
                {
                    if (endpoint == null) continue;

                    String endpointId  = endpoint.vpcEndpointId();
                    String domainArn   = endpoint.domainArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("vpcEndpointId", endpointId);
                    attributes.put("domainArn", domainArn);
                    attributes.put("vpcEndpointOwner", endpoint.vpcEndpointOwner());
                    attributes.put("status", endpoint.statusAsString());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(endpointId)
                            .qualifiedResourceName(endpointId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(endpointId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(endpoint)
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
        catch (OpenSearchException e)
        {
            log.error("OpenSearchVpcEndpointCollector OpenSearch error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect OpenSearch VPC endpoints", e);
        }
        catch (Exception e)
        {
            log.error("OpenSearchVpcEndpointCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting OpenSearch VPC endpoints", e);
        }
    }
}
