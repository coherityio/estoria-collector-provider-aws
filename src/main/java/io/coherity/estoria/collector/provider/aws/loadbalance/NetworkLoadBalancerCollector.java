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
 * Collects Network Load Balancers (NLB) via the ELBv2 DescribeLoadBalancers API,
 * filtered to type=network.
 */
@Slf4j
public class NetworkLoadBalancerCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "NetworkLoadBalancer";

    private ElasticLoadBalancingV2Client elbV2Client;

    public NetworkLoadBalancerCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("loadbalance", "nlb", "network", "aws")).build());
        log.debug("NetworkLoadBalancerCollector created");
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
        log.debug("NetworkLoadBalancerCollector.collect called");

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
                log.debug("NetworkLoadBalancerCollector resuming from nextMarker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeLoadBalancersResponse response = this.elbV2Client.describeLoadBalancers(requestBuilder.build());
            String nextMarker = response.nextMarker();

            List<LoadBalancer> lbs = response.loadBalancers().stream()
                .filter(lb -> LoadBalancerTypeEnum.NETWORK.equals(lb.type()))
                .collect(Collectors.toList());

            log.debug("NetworkLoadBalancerCollector received {} NLBs, nextMarker={}", lbs.size(), nextMarker);

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
            log.error("NetworkLoadBalancerCollector ELBv2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect network load balancers", e);
        }
        catch (Exception e)
        {
            log.error("NetworkLoadBalancerCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting network load balancers", e);
        }
    }
}
