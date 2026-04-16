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
import software.amazon.awssdk.services.ec2.model.DescribeNatGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNatGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.NatGateway;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * NAT Gateway collector for AWS backed by the EC2 DescribeNatGateways API.
 */
@Slf4j
public class NatGatewayCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "NatGateway";


	public NatGatewayCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("Vpc", "Subnet"), Set.of("networking", "vpc", "aws")).build());
		log.debug("NatGatewayCollector.NatGatewayCollector creating NatGatewayCollector");
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
		log.debug("NatGatewayCollector.collectEntities called with request: {}", collectorRequestParams);

		Ec2Client ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("NatGatewayCollector.collect using region: {}", region);

			DescribeNatGatewaysRequest.Builder requestBuilder = DescribeNatGatewaysRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("NatGatewayCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeNatGatewaysRequest describeRequest = requestBuilder.build();
			log.debug("NatGatewayCollector.collect calling DescribeNatGateways with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeNatGatewaysResponse response = ec2Client.describeNatGateways(describeRequest);
			List<NatGateway> natGateways = response.natGateways();
			String nextToken = response.nextToken();

			log.debug("NatGatewayCollector.collect received {} NAT gateways, nextToken={}",
				natGateways != null ? natGateways.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (natGateways != null)
			{
				for (NatGateway natGateway : natGateways)
				{
					if (natGateway == null)
					{
						continue;
					}

					String natGatewayId = natGateway.natGatewayId();
					String vpcId = natGateway.vpcId();
					// NatGateway does not expose ownerId directly; use vpcId as key context
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2NatGatewayArn(regionName, null, natGatewayId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("natGatewayId", natGatewayId);
					attributes.put("vpcId", vpcId);
					attributes.put("subnetId", natGateway.subnetId());
					attributes.put("state", natGateway.stateAsString());
					attributes.put("connectivityType", natGateway.connectivityTypeAsString());

					Map<String, String> tags = natGateway.tags() == null ? Map.of()
						: natGateway.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", natGatewayId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(natGatewayId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(natGateway)
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
			log.error("NatGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect NatGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("NatGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting NatGateways", e);
		}
	}
}
