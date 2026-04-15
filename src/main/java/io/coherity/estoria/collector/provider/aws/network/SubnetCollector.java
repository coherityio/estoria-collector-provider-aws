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
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Subnet;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Subnet collector for AWS backed by the EC2 DescribeSubnets API.
 */
@Slf4j
public class SubnetCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "Subnet";

	private Ec2Client ec2Client;

	public SubnetCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("Vpc"), Set.of("networking", "vpc", "aws")).build());
		log.debug("SubnetCollector.SubnetCollector creating SubnetCollector");
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
		log.debug("SubnetCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("SubnetCollector.collect using region: {}", region);

			DescribeSubnetsRequest.Builder requestBuilder = DescribeSubnetsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SubnetCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeSubnetsRequest describeRequest = requestBuilder.build();
			log.debug("SubnetCollector.collect calling DescribeSubnets with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeSubnetsResponse response = this.ec2Client.describeSubnets(describeRequest);
			List<Subnet> subnets = response.subnets();
			String nextToken = response.nextToken();

			log.debug("SubnetCollector.collect received {} subnets, nextToken={}",
				subnets != null ? subnets.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (subnets != null)
			{
				for (Subnet subnet : subnets)
				{
					if (subnet == null)
					{
						continue;
					}

					String subnetId = subnet.subnetId();
					String ownerId = subnet.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2SubnetArn(regionName, ownerId, subnetId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("subnetId", subnetId);
					attributes.put("vpcId", subnet.vpcId());
					attributes.put("cidrBlock", subnet.cidrBlock());
					attributes.put("availabilityZone", subnet.availabilityZone());
					attributes.put("state", subnet.stateAsString());
					attributes.put("ownerId", ownerId);
					attributes.put("availableIpAddressCount", subnet.availableIpAddressCount());
					attributes.put("defaultForAz", subnet.defaultForAz());
					attributes.put("mapPublicIpOnLaunch", subnet.mapPublicIpOnLaunch());

					Map<String, String> tags = subnet.tags() == null ? Map.of()
						: subnet.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", subnetId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(subnetId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(subnet)
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
			log.error("SubnetCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect Subnets from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("SubnetCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting Subnets", e);
		}
	}
}
