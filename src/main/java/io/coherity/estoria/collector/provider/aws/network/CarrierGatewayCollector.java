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
import software.amazon.awssdk.services.ec2.model.CarrierGateway;
import software.amazon.awssdk.services.ec2.model.DescribeCarrierGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeCarrierGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Carrier Gateway collector for AWS backed by the EC2 DescribeCarrierGateways API.
 */
@Slf4j
public class CarrierGatewayCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "CarrierGateway";

	private Ec2Client ec2Client;

	public CarrierGatewayCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("Vpc"), Set.of("networking", "vpc", "aws", "wavelength")).build());
		log.debug("CarrierGatewayCollector.CarrierGatewayCollector creating CarrierGatewayCollector");
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
		log.debug("CarrierGatewayCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("CarrierGatewayCollector.collect using region: {}", region);

			DescribeCarrierGatewaysRequest.Builder requestBuilder = DescribeCarrierGatewaysRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("CarrierGatewayCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeCarrierGatewaysRequest describeRequest = requestBuilder.build();
			log.debug("CarrierGatewayCollector.collect calling DescribeCarrierGateways with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeCarrierGatewaysResponse response = this.ec2Client.describeCarrierGateways(describeRequest);
			List<CarrierGateway> carrierGateways = response.carrierGateways();
			String nextToken = response.nextToken();

			log.debug("CarrierGatewayCollector.collect received {} carrier gateways, nextToken={}",
				carrierGateways != null ? carrierGateways.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (carrierGateways != null)
			{
				for (CarrierGateway cgw : carrierGateways)
				{
					if (cgw == null)
					{
						continue;
					}

					String cgwId = cgw.carrierGatewayId();
					String ownerId = cgw.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2CarrierGatewayArn(regionName, ownerId, cgwId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("carrierGatewayId", cgwId);
					attributes.put("vpcId", cgw.vpcId());
					attributes.put("state", cgw.stateAsString());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = cgw.tags() == null ? Map.of()
						: cgw.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", cgwId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(cgwId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(cgw)
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
			log.error("CarrierGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect CarrierGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("CarrierGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting CarrierGateways", e);
		}
	}
}
