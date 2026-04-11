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
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.NetworkInterface;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Network Interface (ENI) collector for AWS backed by the EC2 DescribeNetworkInterfaces API.
 */
@Slf4j
public class NetworkInterfaceCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "NetworkInterface";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc", "Subnet"))
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public NetworkInterfaceCollector()
	{
		log.debug("NetworkInterfaceCollector.NetworkInterfaceCollector creating NetworkInterfaceCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("NetworkInterfaceCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
		return ContainmentScope.VPC;
	}

	@Override
	public EntityCategory getEntityCategory()
	{
		return EntityCategory.RESOURCE;
	}

	@Override
	public CollectorCursor collectEntities(ProviderContext providerContext, AwsSessionContext awsSessionContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("NetworkInterfaceCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("NetworkInterfaceCollector.collect using region: {}", region);

			DescribeNetworkInterfacesRequest.Builder requestBuilder = DescribeNetworkInterfacesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("NetworkInterfaceCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeNetworkInterfacesRequest describeRequest = requestBuilder.build();
			log.debug("NetworkInterfaceCollector.collect calling DescribeNetworkInterfaces with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeNetworkInterfacesResponse response = this.ec2Client.describeNetworkInterfaces(describeRequest);
			List<NetworkInterface> networkInterfaces = response.networkInterfaces();
			String nextToken = response.nextToken();

			log.debug("NetworkInterfaceCollector.collect received {} network interfaces, nextToken={}",
				networkInterfaces != null ? networkInterfaces.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (networkInterfaces != null)
			{
				for (NetworkInterface eni : networkInterfaces)
				{
					if (eni == null)
					{
						continue;
					}

					String eniId = eni.networkInterfaceId();
					String ownerId = eni.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2NetworkInterfaceArn(regionName, ownerId, eniId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("networkInterfaceId", eniId);
					attributes.put("vpcId", eni.vpcId());
					attributes.put("subnetId", eni.subnetId());
					attributes.put("availabilityZone", eni.availabilityZone());
					attributes.put("description", eni.description());
					attributes.put("interfaceType", eni.interfaceTypeAsString());
					attributes.put("macAddress", eni.macAddress());
					attributes.put("privateIpAddress", eni.privateIpAddress());
					attributes.put("status", eni.statusAsString());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = eni.tagSet() == null ? Map.of()
						: eni.tagSet().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", eniId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(eniId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(eni)
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
			log.error("NetworkInterfaceCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect NetworkInterfaces from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("NetworkInterfaceCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting NetworkInterfaces", e);
		}
	}
}
