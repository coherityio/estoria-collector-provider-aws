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
import software.amazon.awssdk.services.ec2.model.DescribeTransitGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeTransitGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TransitGateway;

/**
 * Transit Gateway collector for AWS backed by the EC2 DescribeTransitGateways API.
 */
@Slf4j
public class TransitGatewayCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "TransitGateway";

	private Ec2Client ec2Client;

	public TransitGatewayCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("networking", "transit-gateway", "aws")).build());
		log.debug("TransitGatewayCollector.TransitGatewayCollector creating TransitGatewayCollector");
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
		log.debug("TransitGatewayCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("TransitGatewayCollector.collect using region: {}", region);

			DescribeTransitGatewaysRequest.Builder requestBuilder = DescribeTransitGatewaysRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("TransitGatewayCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeTransitGatewaysRequest describeRequest = requestBuilder.build();
			log.debug("TransitGatewayCollector.collect calling DescribeTransitGateways with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeTransitGatewaysResponse response = this.ec2Client.describeTransitGateways(describeRequest);
			List<TransitGateway> tgws = response.transitGateways();
			String nextToken = response.nextToken();

			log.debug("TransitGatewayCollector.collect received {} transit gateways, nextToken={}",
				tgws != null ? tgws.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (tgws != null)
			{
				for (TransitGateway tgw : tgws)
				{
					if (tgw == null)
					{
						continue;
					}

					String tgwId = tgw.transitGatewayId();
					String ownerId = tgw.ownerId();
					String regionName = region != null ? region.id() : null;
					// Transit Gateway has its own ARN field
					String arn = tgw.transitGatewayArn() != null ? tgw.transitGatewayArn()
						: ARNHelper.ec2TransitGatewayArn(regionName, ownerId, tgwId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("transitGatewayId", tgwId);
					attributes.put("transitGatewayArn", tgw.transitGatewayArn());
					attributes.put("ownerId", ownerId);
					attributes.put("state", tgw.stateAsString());
					attributes.put("description", tgw.description());

					Map<String, String> tags = tgw.tags() == null ? Map.of()
						: tgw.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", tgwId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(tgwId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(tgw)
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
			log.error("TransitGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect TransitGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("TransitGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting TransitGateways", e);
		}
	}
}
