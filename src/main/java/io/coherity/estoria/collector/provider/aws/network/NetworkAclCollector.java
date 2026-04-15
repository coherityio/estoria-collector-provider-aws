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
import software.amazon.awssdk.services.ec2.model.DescribeNetworkAclsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeNetworkAclsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.NetworkAcl;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Network ACL collector for AWS backed by the EC2 DescribeNetworkAcls API.
 */
@Slf4j
public class NetworkAclCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "NetworkAcl";

	private Ec2Client ec2Client;

	public NetworkAclCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("Vpc"), Set.of("networking", "vpc", "aws", "security")).build());
		log.debug("NetworkAclCollector.NetworkAclCollector creating NetworkAclCollector");
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
		log.debug("NetworkAclCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("NetworkAclCollector.collect using region: {}", region);

			DescribeNetworkAclsRequest.Builder requestBuilder = DescribeNetworkAclsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("NetworkAclCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeNetworkAclsRequest describeRequest = requestBuilder.build();
			log.debug("NetworkAclCollector.collect calling DescribeNetworkAcls with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeNetworkAclsResponse response = this.ec2Client.describeNetworkAcls(describeRequest);
			List<NetworkAcl> networkAcls = response.networkAcls();
			String nextToken = response.nextToken();

			log.debug("NetworkAclCollector.collect received {} network ACLs, nextToken={}",
				networkAcls != null ? networkAcls.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (networkAcls != null)
			{
				for (NetworkAcl acl : networkAcls)
				{
					if (acl == null)
					{
						continue;
					}

					String networkAclId = acl.networkAclId();
					String ownerId = acl.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2NetworkAclArn(regionName, ownerId, networkAclId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("networkAclId", networkAclId);
					attributes.put("vpcId", acl.vpcId());
					attributes.put("isDefault", acl.isDefault());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = acl.tags() == null ? Map.of()
						: acl.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", networkAclId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(networkAclId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(acl)
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
			log.error("NetworkAclCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect NetworkAcls from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("NetworkAclCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting NetworkAcls", e);
		}
	}
}
