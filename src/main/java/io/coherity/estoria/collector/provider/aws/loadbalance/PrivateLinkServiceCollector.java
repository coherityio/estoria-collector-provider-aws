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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointServiceConfigurationsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointServiceConfigurationsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.ServiceConfiguration;

/**
 * Collects VPC endpoint service configurations (PrivateLink services that this account exposes)
 * via the EC2 DescribeVpcEndpointServiceConfigurations API.
 */
@Slf4j
public class PrivateLinkServiceCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID  = "aws";
    public  static final String ENTITY_TYPE  = "PrivateLinkService";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("loadbalance", "privatelink", "endpoint-service", "aws"))
            .build();

    public PrivateLinkServiceCollector()
    {
        log.debug("PrivateLinkServiceCollector created");
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
        log.debug("PrivateLinkServiceCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            DescribeVpcEndpointServiceConfigurationsRequest.Builder requestBuilder =
                DescribeVpcEndpointServiceConfigurationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("PrivateLinkServiceCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeVpcEndpointServiceConfigurationsResponse response =
                this.ec2Client.describeVpcEndpointServiceConfigurations(requestBuilder.build());
            List<ServiceConfiguration> services = response.serviceConfigurations();
            String nextToken = response.nextToken();

            log.debug("PrivateLinkServiceCollector received {} endpoint service configs, nextToken={}",
                services != null ? services.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (services != null)
            {
                for (ServiceConfiguration svc : services)
                {
                    if (svc == null) continue;

                    String serviceId = svc.serviceId();
                    String arn = ARNHelper.ec2PrivateLinkServiceArn(region, accountId, serviceId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("serviceId", serviceId);
                    attributes.put("serviceName", svc.serviceName());
                    attributes.put("serviceState", svc.serviceStateAsString());
                    attributes.put("serviceType", svc.serviceType() != null && !svc.serviceType().isEmpty()
                        ? svc.serviceType().get(0).serviceTypeAsString() : null);
                    attributes.put("acceptanceRequired", svc.acceptanceRequired());
                    attributes.put("availabilityZones", svc.availabilityZones());
                    attributes.put("networkLoadBalancerArns", svc.networkLoadBalancerArns());
                    attributes.put("gatewayLoadBalancerArns", svc.gatewayLoadBalancerArns());
                    attributes.put("privateDnsName", svc.privateDnsName());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(serviceId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(svc.serviceName() != null ? svc.serviceName() : serviceId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(svc)
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
        catch (Ec2Exception e)
        {
            log.error("PrivateLinkServiceCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect PrivateLink endpoint service configurations", e);
        }
        catch (Exception e)
        {
            log.error("PrivateLinkServiceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting PrivateLink services", e);
        }
    }

}
