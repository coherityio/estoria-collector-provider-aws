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
import software.amazon.awssdk.services.ec2.model.DescribeLocalGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeLocalGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.LocalGateway;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Local Gateway collector for AWS backed by the EC2 DescribeLocalGateways API.
 */
@Slf4j
public class LocalGatewayCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "LocalGateway";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of())
				.tags(Set.of("networking", "outpost", "aws"))
				.build();

	public LocalGatewayCollector()
	{
		log.debug("LocalGatewayCollector.LocalGatewayCollector creating LocalGatewayCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("LocalGatewayCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
		log.debug("LocalGatewayCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("LocalGatewayCollector.collect using region: {}", region);

			DescribeLocalGatewaysRequest.Builder requestBuilder = DescribeLocalGatewaysRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("LocalGatewayCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeLocalGatewaysRequest describeRequest = requestBuilder.build();
			log.debug("LocalGatewayCollector.collect calling DescribeLocalGateways with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeLocalGatewaysResponse response = this.ec2Client.describeLocalGateways(describeRequest);
			List<LocalGateway> localGateways = response.localGateways();
			String nextToken = response.nextToken();

			log.debug("LocalGatewayCollector.collect received {} local gateways, nextToken={}",
				localGateways != null ? localGateways.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (localGateways != null)
			{
				for (LocalGateway lgw : localGateways)
				{
					if (lgw == null)
					{
						continue;
					}

					String lgwId = lgw.localGatewayId();
					String ownerId = lgw.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2LocalGatewayArn(regionName, ownerId, lgwId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("localGatewayId", lgwId);
					attributes.put("outpostArn", lgw.outpostArn());
					attributes.put("state", lgw.state());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = lgw.tags() == null ? Map.of()
						: lgw.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", lgwId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(lgwId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(lgw)
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
			log.error("LocalGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect LocalGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("LocalGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting LocalGateways", e);
		}
	}
}
