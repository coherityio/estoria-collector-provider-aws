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
import software.amazon.awssdk.services.ec2.model.DescribeVpnConnectionsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpnConnectionsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.VpnConnection;

/**
 * VPN Connection collector for AWS backed by the EC2 DescribeVpnConnections API.
 * Note: DescribeVpnConnections does not support pagination; it returns all results.
 */
@Slf4j
public class VpnConnectionCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "VpnConnection";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("CustomerGateway"))
				.tags(Set.of("networking", "vpn", "aws"))
				.build();

	public VpnConnectionCollector()
	{
		log.debug("VpnConnectionCollector.VpnConnectionCollector creating VpnConnectionCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("VpnConnectionCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
		log.debug("VpnConnectionCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("VpnConnectionCollector.collect using region: {}", region);

			// DescribeVpnConnections does not support pagination
			DescribeVpnConnectionsRequest describeRequest = DescribeVpnConnectionsRequest.builder().build();
			log.debug("VpnConnectionCollector.collect calling DescribeVpnConnections");
			DescribeVpnConnectionsResponse response = this.ec2Client.describeVpnConnections(describeRequest);
			List<VpnConnection> vpnConnections = response.vpnConnections();

			log.debug("VpnConnectionCollector.collect received {} VPN connections",
				vpnConnections != null ? vpnConnections.size() : 0);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (vpnConnections != null)
			{
				for (VpnConnection vpn : vpnConnections)
				{
					if (vpn == null)
					{
						continue;
					}

					String vpnId = vpn.vpnConnectionId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2VpnConnectionArn(regionName, null, vpnId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("vpnConnectionId", vpnId);
					attributes.put("customerGatewayId", vpn.customerGatewayId());
					attributes.put("vpnGatewayId", vpn.vpnGatewayId());
					attributes.put("transitGatewayId", vpn.transitGatewayId());
					attributes.put("state", vpn.stateAsString());
					attributes.put("type", vpn.typeAsString());
					attributes.put("category", vpn.category());

					Map<String, String> tags = vpn.tags() == null ? Map.of()
						: vpn.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", vpnId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(vpnId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(vpn)
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
			log.error("VpnConnectionCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect VpnConnections from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("VpnConnectionCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting VpnConnections", e);
		}
	}
}
