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
import software.amazon.awssdk.services.ec2.model.DescribeRouteTablesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeRouteTablesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.RouteTable;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Route Table collector for AWS backed by the EC2 DescribeRouteTables API.
 */
@Slf4j
public class RouteTableCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "RouteTable";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc"))
				.tags(Set.of("networking", "vpc", "aws"))
				.build();

	public RouteTableCollector()
	{
		log.debug("RouteTableCollector.RouteTableCollector creating RouteTableCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("RouteTableCollector.getCollectorInfo called - returning {}", this.collectorInfo);
		return this.collectorInfo;
	}

	@Override
	public CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("RouteTableCollector.collect called with request: {}", collectorRequestParams);

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

			log.debug("RouteTableCollector.collect using region: {}", region);

			DescribeRouteTablesRequest.Builder requestBuilder = DescribeRouteTablesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("RouteTableCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeRouteTablesRequest describeRequest = requestBuilder.build();
			log.debug("RouteTableCollector.collect calling DescribeRouteTables with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeRouteTablesResponse response = this.ec2Client.describeRouteTables(describeRequest);
			List<RouteTable> routeTables = response.routeTables();
			String nextToken = response.nextToken();

			log.debug("RouteTableCollector.collect received {} route tables, nextToken={}",
				routeTables != null ? routeTables.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (routeTables != null)
			{
				for (RouteTable routeTable : routeTables)
				{
					if (routeTable == null)
					{
						continue;
					}

					String routeTableId = routeTable.routeTableId();
					String ownerId = routeTable.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2RouteTableArn(regionName, ownerId, routeTableId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("routeTableId", routeTableId);
					attributes.put("vpcId", routeTable.vpcId());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = routeTable.tags() == null ? Map.of()
						: routeTable.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", routeTableId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(routeTableId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(routeTable)
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
			log.error("RouteTableCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect RouteTables from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("RouteTableCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting RouteTables", e);
		}
	}
}
