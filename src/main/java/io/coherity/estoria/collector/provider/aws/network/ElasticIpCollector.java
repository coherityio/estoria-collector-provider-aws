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
import software.amazon.awssdk.services.ec2.model.Address;
import software.amazon.awssdk.services.ec2.model.DescribeAddressesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeAddressesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Elastic IP (EIP) collector for AWS backed by the EC2 DescribeAddresses API.
 * Note: DescribeAddresses does not support pagination; it returns all results.
 */
@Slf4j
public class ElasticIpCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "ElasticIp";

	private Ec2Client ec2Client;

	public ElasticIpCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("networking", "vpc", "aws")).build());
		log.debug("ElasticIpCollector.ElasticIpCollector creating ElasticIpCollector");
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
	public CollectorCursor collectEntities(ProviderContext providerContext, AwsSessionContext awsSessionContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("ElasticIpCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("ElasticIpCollector.collect using region: {}", region);

			// DescribeAddresses does not support pagination
			DescribeAddressesRequest describeRequest = DescribeAddressesRequest.builder().build();
			log.debug("ElasticIpCollector.collect calling DescribeAddresses");
			DescribeAddressesResponse response = this.ec2Client.describeAddresses(describeRequest);
			List<Address> addresses = response.addresses();

			log.debug("ElasticIpCollector.collect received {} Elastic IPs",
				addresses != null ? addresses.size() : 0);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (addresses != null)
			{
				for (Address address : addresses)
				{
					if (address == null)
					{
						continue;
					}

					String allocationId = address.allocationId();
					// For EC2-Classic, allocationId may be null; fall back to publicIp
					String entityId = allocationId != null ? allocationId : address.publicIp();
					String regionName = region != null ? region.id() : null;
					String arn = allocationId != null
						? ARNHelper.ec2ElasticIpArn(regionName, null, allocationId)
						: "arn:" + ARNHelper.partitionForRegion(regionName) + ":ec2:" + (regionName != null ? regionName : "") + "::elastic-ip/" + address.publicIp();

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("allocationId", allocationId);
					attributes.put("publicIp", address.publicIp());
					attributes.put("privateIpAddress", address.privateIpAddress());
					attributes.put("associationId", address.associationId());
					attributes.put("instanceId", address.instanceId());
					attributes.put("networkInterfaceId", address.networkInterfaceId());
					attributes.put("networkInterfaceOwnerId", address.networkInterfaceOwnerId());
					attributes.put("domain", address.domainAsString());
					attributes.put("publicIpv4Pool", address.publicIpv4Pool());

					Map<String, String> tags = address.tags() == null ? Map.of()
						: address.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", entityId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(entityId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(address)
						.collectedAt(now)
						.build();

					entities.add(entity);
				}
			}

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
					return Optional.empty();
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
			log.error("ElasticIpCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect ElasticIps from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("ElasticIpCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting ElasticIps", e);
		}
	}
}
