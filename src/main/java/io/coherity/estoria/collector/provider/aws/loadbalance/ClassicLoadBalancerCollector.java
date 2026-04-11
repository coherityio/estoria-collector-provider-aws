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
import software.amazon.awssdk.services.elasticloadbalancing.ElasticLoadBalancingClient;
import software.amazon.awssdk.services.elasticloadbalancing.model.DescribeLoadBalancersRequest;
import software.amazon.awssdk.services.elasticloadbalancing.model.DescribeLoadBalancersResponse;
import software.amazon.awssdk.services.elasticloadbalancing.model.ElasticLoadBalancingException;
import software.amazon.awssdk.services.elasticloadbalancing.model.LoadBalancerDescription;

/**
 * Collects Classic (v1) ELB load balancers via the ElasticLoadBalancing DescribeLoadBalancers API.
 */
@Slf4j
public class ClassicLoadBalancerCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "ClassicLoadBalancer";

    private ElasticLoadBalancingClient elbClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "elb", "classic", "aws"))
            .build();

    public ClassicLoadBalancerCollector()
    {
        log.debug("ClassicLoadBalancerCollector created");
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
        log.debug("ClassicLoadBalancerCollector.collect called");

        if (this.elbClient == null)
        {
            this.elbClient = AwsClientFactory.getInstance().getElbClient(providerContext);
        }

        try
        {
            String region     = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId  = awsSessionContext.getCurrentAccountId();

            DescribeLoadBalancersRequest.Builder requestBuilder = DescribeLoadBalancersRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.pageSize(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("ClassicLoadBalancerCollector resuming from nextMarker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeLoadBalancersResponse response = this.elbClient.describeLoadBalancers(requestBuilder.build());
            List<LoadBalancerDescription> lbs = response.loadBalancerDescriptions();
            String nextMarker = response.nextMarker();

            log.debug("ClassicLoadBalancerCollector received {} classic LBs, nextMarker={}",
                lbs != null ? lbs.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (lbs != null)
            {
                for (LoadBalancerDescription lb : lbs)
                {
                    if (lb == null) continue;

                    String lbName = lb.loadBalancerName();
                    String arn = ARNHelper.elbClassicArn(region, accountId, lbName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("loadBalancerName", lbName);
                    attributes.put("dnsName", lb.dnsName());
                    attributes.put("scheme", lb.scheme());
                    attributes.put("vpcId", lb.vpcId());
                    attributes.put("availabilityZones", lb.availabilityZones());
                    attributes.put("subnets", lb.subnets());
                    attributes.put("securityGroups", lb.securityGroups());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(lbName)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(lbName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(lb)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
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
        catch (ElasticLoadBalancingException e)
        {
            log.error("ClassicLoadBalancerCollector ELB error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect classic load balancers", e);
        }
        catch (Exception e)
        {
            log.error("ClassicLoadBalancerCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting classic load balancers", e);
        }
    }

}
