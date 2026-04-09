package io.coherity.estoria.collector.provider.aws.network;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
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
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcEndpointsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.VpcEndpoint;

/**
 * VPC Endpoint collector for AWS backed by the EC2 DescribeVpcEndpoints API.
 */
@Slf4j
public class VpcEndpointCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "VpcEndpoint";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc"))
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public VpcEndpointCollector()
	{
		log.debug("VpcEndpointCollector.VpcEndpointCollector creating VpcEndpointCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("VpcEndpointCollector.getCollectorInfo called - returning {}", this.collectorInfo);
		return this.collectorInfo;
	}

	@Override
	public CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("VpcEndpointCollector.collect called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			String regionFromProviderContext = null;
			if (providerContext != null && providerContext.getAttributes() != null)
			{
				regionFromProviderContext = providerContext.getAttributes().get("region").toString();
			}

			Region region = (StringUtils.isNotEmpty(regionFromProviderContext))
				? Region.of(regionFromProviderContext)
				: null;

			log.debug("VpcEndpointCollector.collect using region: {}", region);

			DescribeVpcEndpointsRequest.Builder requestBuilder = DescribeVpcEndpointsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("VpcEndpointCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeVpcEndpointsRequest describeRequest = requestBuilder.build();
			log.debug("VpcEndpointCollector.collect calling DescribeVpcEndpoints with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeVpcEndpointsResponse response = this.ec2Client.describeVpcEndpoints(describeRequest);
			List<VpcEndpoint> endpoints = response.vpcEndpoints();
			String nextToken = response.nextToken();

			log.debug("VpcEndpointCollector.collect received {} VPC endpoints, nextToken={}",
				endpoints != null ? endpoints.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (endpoints != null)
			{
				for (VpcEndpoint endpoint : endpoints)
				{
					if (endpoint == null)
					{
						continue;
					}

					String endpointId = endpoint.vpcEndpointId();
					String ownerId = endpoint.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2VpcEndpointArn(regionName, ownerId, endpointId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("vpcEndpointId", endpointId);
					attributes.put("vpcId", endpoint.vpcId());
					attributes.put("serviceName", endpoint.serviceName());
					attributes.put("state", endpoint.stateAsString());
					attributes.put("vpcEndpointType", endpoint.vpcEndpointTypeAsString());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = endpoint.tags() == null ? Map.of()
						: endpoint.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", endpointId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(endpointId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
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
			log.error("VpcEndpointCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect VpcEndpoints from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("VpcEndpointCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting VpcEndpoints", e);
		}
	}
}
