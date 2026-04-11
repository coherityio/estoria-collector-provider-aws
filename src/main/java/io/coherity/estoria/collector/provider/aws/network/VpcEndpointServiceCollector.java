package io.coherity.estoria.collector.provider.aws.network;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointServicesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointServicesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.ServiceDetail;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * VPC Endpoint Service collector for AWS backed by the EC2 DescribeVpcEndpointServices API.
 */
@Slf4j
public class VpcEndpointServiceCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "VpcEndpointService";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of())
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public VpcEndpointServiceCollector()
	{
		log.debug("VpcEndpointServiceCollector.VpcEndpointServiceCollector creating VpcEndpointServiceCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("VpcEndpointServiceCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
		return EntityCategory.REFERENCE;
	}

	@Override
	public CollectorCursor collectEntities(ProviderContext providerContext, AwsSessionContext awsSessionContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("VpcEndpointServiceCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("VpcEndpointServiceCollector.collect using region: {}", region);

			DescribeVpcEndpointServicesRequest.Builder requestBuilder = DescribeVpcEndpointServicesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("VpcEndpointServiceCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeVpcEndpointServicesRequest describeRequest = requestBuilder.build();
			log.debug("VpcEndpointServiceCollector.collect calling DescribeVpcEndpointServices with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeVpcEndpointServicesResponse response = this.ec2Client.describeVpcEndpointServices(describeRequest);
			List<ServiceDetail> services = response.serviceDetails();
			String nextToken = response.nextToken();

			log.debug("VpcEndpointServiceCollector.collect received {} VPC endpoint services, nextToken={}",
				services != null ? services.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (services != null)
			{
				for (ServiceDetail service : services)
				{
					if (service == null)
					{
						continue;
					}

					String serviceId = service.serviceId();
					String serviceName = service.serviceName();
					String regionName = region != null ? region.id() : null;
					// No standard ARN for endpoint services; construct a pseudo-ARN using serviceId
					String arn = "arn:" + ARNHelper.partitionForRegion(regionName) + ":ec2:" + (regionName != null ? regionName : "") + "::vpc-endpoint-service/" + serviceId;

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("serviceId", serviceId);
					attributes.put("serviceName", serviceName);
					attributes.put("serviceType", service.serviceType() != null && !service.serviceType().isEmpty()
						? service.serviceType().get(0).serviceTypeAsString() : null);
					attributes.put("owner", service.owner());
					attributes.put("acceptanceRequired", service.acceptanceRequired());
					attributes.put("managesVpcEndpoints", service.managesVpcEndpoints());

					Map<String, String> tags = service.tags() == null ? Map.of()
						: service.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", serviceName != null ? serviceName : serviceId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(serviceId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(service)
						.collectedAt(now)
						.build();

					entities.add(entity);
				}
			}

			String finalNextToken = nextToken;
			Map<String, Object> metadataValues = new HashMap<>();
			metadataValues.put("count", entities.size());

			CursorMetadata metadata = CursorMetadata.builder()
				.values(metadataValues)
				.build();

			return new CollectorCursor()
			{
				@Override
				public List<CloudEntity> getEntities()
				{
					return entities;
				}

				@Override
				public Optional<String> getNextCursorToken()
				{
					return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
				}

				@Override
				public CursorMetadata getMetadata()
				{
					return metadata;
				}
			};
		}
		catch (Ec2Exception e)
		{
			log.error("VpcEndpointServiceCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect VpcEndpointServices from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("VpcEndpointServiceCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting VpcEndpointServices", e);
		}
	}
}
