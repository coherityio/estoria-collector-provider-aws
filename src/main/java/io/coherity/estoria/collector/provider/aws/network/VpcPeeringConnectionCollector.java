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
import software.amazon.awssdk.services.ec2.model.DescribeVpcPeeringConnectionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcPeeringConnectionsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.VpcPeeringConnection;

/**
 * VPC Peering Connection collector for AWS backed by the EC2 DescribeVpcPeeringConnections API.
 */
@Slf4j
public class VpcPeeringConnectionCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "VpcPeeringConnection";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc"))
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public VpcPeeringConnectionCollector()
	{
		log.debug("VpcPeeringConnectionCollector.VpcPeeringConnectionCollector creating VpcPeeringConnectionCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("VpcPeeringConnectionCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
	public CollectorCursor collectEntities(ProviderContext providerContext, AwsSessionContext awsSessionContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("VpcPeeringConnectionCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("VpcPeeringConnectionCollector.collect using region: {}", region);

			DescribeVpcPeeringConnectionsRequest.Builder requestBuilder = DescribeVpcPeeringConnectionsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("VpcPeeringConnectionCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeVpcPeeringConnectionsRequest describeRequest = requestBuilder.build();
			log.debug("VpcPeeringConnectionCollector.collect calling DescribeVpcPeeringConnections with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeVpcPeeringConnectionsResponse response = this.ec2Client.describeVpcPeeringConnections(describeRequest);
			List<VpcPeeringConnection> peerings = response.vpcPeeringConnections();
			String nextToken = response.nextToken();

			log.debug("VpcPeeringConnectionCollector.collect received {} VPC peering connections, nextToken={}",
				peerings != null ? peerings.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (peerings != null)
			{
				for (VpcPeeringConnection peering : peerings)
				{
					if (peering == null)
					{
						continue;
					}

					String peeringId = peering.vpcPeeringConnectionId();
					String regionName = region != null ? region.id() : null;
					String requesterOwnerId = peering.requesterVpcInfo() != null ? peering.requesterVpcInfo().ownerId() : null;
					String arn = ARNHelper.ec2VpcPeeringConnectionArn(regionName, requesterOwnerId, peeringId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("vpcPeeringConnectionId", peeringId);
					attributes.put("status", peering.status() != null ? peering.status().codeAsString() : null);
					if (peering.requesterVpcInfo() != null)
					{
						attributes.put("requesterVpcId", peering.requesterVpcInfo().vpcId());
						attributes.put("requesterOwnerId", peering.requesterVpcInfo().ownerId());
						attributes.put("requesterRegion", peering.requesterVpcInfo().region());
					}
					if (peering.accepterVpcInfo() != null)
					{
						attributes.put("accepterVpcId", peering.accepterVpcInfo().vpcId());
						attributes.put("accepterOwnerId", peering.accepterVpcInfo().ownerId());
						attributes.put("accepterRegion", peering.accepterVpcInfo().region());
					}

					Map<String, String> tags = peering.tags() == null ? Map.of()
						: peering.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", peeringId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(peeringId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(peering)
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
			log.error("VpcPeeringConnectionCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect VpcPeeringConnections from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("VpcPeeringConnectionCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting VpcPeeringConnections", e);
		}
	}
}
