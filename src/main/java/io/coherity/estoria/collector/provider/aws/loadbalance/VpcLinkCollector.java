package io.coherity.estoria.collector.provider.aws.loadbalance;

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
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.apigateway.model.ApiGatewayException;
import software.amazon.awssdk.services.apigateway.model.GetVpcLinksRequest;
import software.amazon.awssdk.services.apigateway.model.GetVpcLinksResponse;
import software.amazon.awssdk.services.apigateway.model.VpcLink;

/**
 * Collects API Gateway VPC Links via the GetVpcLinks API.
 * VPC Links allow API Gateway to privately connect to NLBs inside a VPC.
 */
@Slf4j
public class VpcLinkCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "VpcLink";

    private ApiGatewayClient apiGatewayClient;

    public VpcLinkCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("loadbalance", "vpc-link", "api-gateway", "aws")).build());
        log.debug("VpcLinkCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.ACCOUNT_REGIONAL;
    }

    @Override
    public EntityCategory getEntityCategory()
    {
        return EntityCategory.RESOURCE;
    }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("VpcLinkCollector.collect called");

        if (this.apiGatewayClient == null)
        {
            this.apiGatewayClient = AwsClientFactory.getInstance().getApiGatewayClient(providerContext);
        }

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            GetVpcLinksRequest.Builder requestBuilder = GetVpcLinksRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.limit(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("VpcLinkCollector resuming from position: {}", token);
                requestBuilder.position(token);
            });

            GetVpcLinksResponse response = this.apiGatewayClient.getVpcLinks(requestBuilder.build());
            List<VpcLink> vpcLinks = response.items();
            String nextPosition = response.position();

            log.debug("VpcLinkCollector received {} VPC links, nextPosition={}",
                vpcLinks != null ? vpcLinks.size() : 0, nextPosition);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (vpcLinks != null)
            {
                for (VpcLink link : vpcLinks)
                {
                    if (link == null) continue;

                    String id  = link.id();
                    String arn = ARNHelper.apiGatewayVpcLinkArn(region, id);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", id);
                    attributes.put("name", link.name());
                    attributes.put("description", link.description());
                    attributes.put("status", link.statusAsString());
                    attributes.put("statusMessage", link.statusMessage());
                    attributes.put("targetArns", link.targetArns());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(id)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(link.name() != null ? link.name() : id)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(link)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextPosition = nextPosition;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextPosition).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (ApiGatewayException e)
        {
            log.error("VpcLinkCollector API Gateway error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect API Gateway VPC links", e);
        }
        catch (Exception e)
        {
            log.error("VpcLinkCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting API Gateway VPC links", e);
        }
    }

}
