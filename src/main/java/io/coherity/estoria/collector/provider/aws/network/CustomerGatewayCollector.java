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
import software.amazon.awssdk.services.ec2.model.CustomerGateway;
import software.amazon.awssdk.services.ec2.model.DescribeCustomerGatewaysRequest;
import software.amazon.awssdk.services.ec2.model.DescribeCustomerGatewaysResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Customer Gateway collector for AWS backed by the EC2 DescribeCustomerGateways API.
 * Note: DescribeCustomerGateways does not support pagination; it returns all results.
 */
@Slf4j
public class CustomerGatewayCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "CustomerGateway";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of())
				.tags(Set.of("networking", "vpn", "aws"))
				.build();

	public CustomerGatewayCollector()
	{
		log.debug("CustomerGatewayCollector.CustomerGatewayCollector creating CustomerGatewayCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("CustomerGatewayCollector.getCollectorInfo called - returning {}", this.collectorInfo);
		return this.collectorInfo;
	}

	@Override
	public CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext, CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("CustomerGatewayCollector.collect called with request: {}", collectorRequestParams);

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

			log.debug("CustomerGatewayCollector.collect using region: {}", region);

			// DescribeCustomerGateways does not support pagination
			DescribeCustomerGatewaysRequest describeRequest = DescribeCustomerGatewaysRequest.builder().build();
			log.debug("CustomerGatewayCollector.collect calling DescribeCustomerGateways");
			DescribeCustomerGatewaysResponse response = this.ec2Client.describeCustomerGateways(describeRequest);
			List<CustomerGateway> customerGateways = response.customerGateways();

			log.debug("CustomerGatewayCollector.collect received {} customer gateways",
				customerGateways != null ? customerGateways.size() : 0);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (customerGateways != null)
			{
				for (CustomerGateway cgw : customerGateways)
				{
					if (cgw == null)
					{
						continue;
					}

					String cgwId = cgw.customerGatewayId();
					String ownerId    = resolveAccountId(providerContext);
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2CustomerGatewayArn(regionName, ownerId, cgwId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("customerGatewayId", cgwId);
					attributes.put("ipAddress", cgw.ipAddress());
					attributes.put("bgpAsn", cgw.bgpAsn());
					attributes.put("state", cgw.state());
					attributes.put("type", cgw.type());
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
			log.error("CustomerGatewayCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect CustomerGateways from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("CustomerGatewayCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting CustomerGateways", e);
		}
	}
	
    private static String resolveAccountId(ProviderContext providerContext)
    {
        if (providerContext != null && providerContext.getAttributes() != null)
        {
            Object found = providerContext.getAttributes().get("accountId");
            if (found != null)
            {
                return found.toString();
            }
        }
        return null;
    }

}
