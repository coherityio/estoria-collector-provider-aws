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
import software.amazon.awssdk.services.ec2.model.DescribeInternetGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInternetGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InternetGateway;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Internet Gateway collector for AWS backed by the EC2 DescribeInternetGateways API.
 */
@Slf4j
public class InternetGatewayCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "InternetGateway";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc"))
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public InternetGatewayCollector()
	{
		log.debug("InternetGatewayCollector.InternetGatewayCollector creating InternetGatewayCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("InternetGatewayCollector.getCollectorInfo called - returning {}", this.collectorInfo);
		return this.collectorInfo;
	}

	@Override
	public CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("InternetGatewayCollector.collect called with request: {}", collectorRequestParams);

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

			log.debug("InternetGatewayCollector.collect using region: {}", region);

			DescribeInternetGatewaysRequest.Builder requestBuilder = DescribeInternetGatewaysRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("InternetGatewayCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeInternetGatewaysRequest describeRequest = requestBuilder.build();
			log.debug("InternetGatewayCollector.collect calling DescribeInternetGateways with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeInternetGatewaysResponse response = this.ec2Client.describeInternetGateways(describeRequest);
			List<InternetGateway> igws = response.internetGateways();
			String nextToken = response.nextToken();

			log.debug("InternetGatewayCollector.collect received {} internet gateways, nextToken={}",
				igws != null ? igws.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (igws != null)
			{
				for (InternetGateway igw : igws)
				{
					if (igw == null)
					{
						continue;
					}

					String igwId = igw.internetGatewayId();
					String ownerId = igw.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2InternetGatewayArn(regionName, ownerId, igwId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("internetGatewayId", igwId);
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = igw.tags() == null ? Map.of()
						: igw.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", igwId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(igwId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(igw)
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
			log.error("InternetGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect InternetGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("InternetGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting InternetGateways", e);
		}
	}
}
