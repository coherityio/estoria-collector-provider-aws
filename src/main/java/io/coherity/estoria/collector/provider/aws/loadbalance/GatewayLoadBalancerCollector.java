package io.coherity.estoria.collector.provider.aws.loadbalance;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2Client;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeLoadBalancersResponse;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.ElasticLoadBalancingV2Exception;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.LoadBalancer;
import software.amazon.awssdk.services.elasticloadbalancingv2.model.LoadBalancerTypeEnum;

/**
 * Collects Gateway Load Balancers (GWLB) via the ELBv2 DescribeLoadBalancers API,
 * filtered to type=gateway.
 */
@Slf4j
public class GatewayLoadBalancerCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "GatewayLoadBalancer";

    private ElasticLoadBalancingV2Client elbV2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "gwlb", "gateway", "aws"))
            .build();

    public GatewayLoadBalancerCollector()
    {
        log.debug("GatewayLoadBalancerCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("GatewayLoadBalancerCollector.collect called");

        if (this.elbV2Client == null)
        {
            this.elbV2Client = AwsClientFactory.getInstance().getElbV2Client(providerContext);
        }

        try
        {
            DescribeLoadBalancersRequest.Builder requestBuilder = DescribeLoadBalancersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.pageSize(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("GatewayLoadBalancerCollector resuming from nextMarker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeLoadBalancersResponse response = this.elbV2Client.describeLoadBalancers(requestBuilder.build());
            String nextMarker = response.nextMarker();

            List<LoadBalancer> lbs = response.loadBalancers().stream()
                .filter(lb -> LoadBalancerTypeEnum.GATEWAY.equals(lb.type()))
                .collect(Collectors.toList());

            log.debug("GatewayLoadBalancerCollector received {} GWLBs, nextMarker={}", lbs.size(), nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (LoadBalancer lb : lbs)
            {
                String arn = lb.loadBalancerArn();

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("loadBalancerArn", arn);
                attributes.put("loadBalancerName", lb.loadBalancerName());
                attributes.put("dnsName", lb.dnsName());
                attributes.put("scheme", lb.schemeAsString());
                attributes.put("state", lb.state() != null ? lb.state().codeAsString() : null);
                attributes.put("vpcId", lb.vpcId());
                attributes.put("type", lb.typeAsString());

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(arn)
                        .qualifiedResourceName(arn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(lb.loadBalancerName())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(lb)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
            }

            String finalNextToken = nextMarker;
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
        catch (ElasticLoadBalancingV2Exception e)
        {
            log.error("GatewayLoadBalancerCollector ELBv2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect gateway load balancers", e);
        }
        catch (Exception e)
        {
            log.error("GatewayLoadBalancerCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting gateway load balancers", e);
        }
    }
}
