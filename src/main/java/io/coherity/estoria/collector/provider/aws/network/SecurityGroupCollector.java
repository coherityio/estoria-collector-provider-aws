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
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.SecurityGroup;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Security Group collector for AWS backed by the EC2 DescribeSecurityGroups API.
 */
@Slf4j
public class SecurityGroupCollector extends AbstractAwsContextAwareCollector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "SecurityGroup";

	private Ec2Client ec2Client;

	private final CollectorInfo collectorInfo =
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of("Vpc"))
				.tags(Set.of("networking", "vpc", "aws", "security"))
				.build();

	public SecurityGroupCollector()
	{
		log.debug("SecurityGroupCollector.SecurityGroupCollector creating SecurityGroupCollector");
	}

	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("SecurityGroupCollector.getCollectorInfo called - returning {}", this.collectorInfo);
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
		log.debug("SecurityGroupCollector.collectEntities called with request: {}", collectorRequestParams);

		if (this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("SecurityGroupCollector.collect using region: {}", region);

			DescribeSecurityGroupsRequest.Builder requestBuilder = DescribeSecurityGroupsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SecurityGroupCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeSecurityGroupsRequest describeRequest = requestBuilder.build();
			log.debug("SecurityGroupCollector.collect calling DescribeSecurityGroups with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeSecurityGroupsResponse response = this.ec2Client.describeSecurityGroups(describeRequest);
			List<SecurityGroup> securityGroups = response.securityGroups();
			String nextToken = response.nextToken();

			log.debug("SecurityGroupCollector.collect received {} security groups, nextToken={}",
				securityGroups != null ? securityGroups.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (securityGroups != null)
			{
				for (SecurityGroup sg : securityGroups)
				{
					if (sg == null)
					{
						continue;
					}

					String groupId = sg.groupId();
					String ownerId = sg.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2SecurityGroupArn(regionName, ownerId, groupId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("groupId", groupId);
					attributes.put("groupName", sg.groupName());
					attributes.put("description", sg.description());
					attributes.put("vpcId", sg.vpcId());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = sg.tags() == null ? Map.of()
						: sg.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", sg.groupName() != null ? sg.groupName() : groupId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(groupId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(sg)
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
			log.error("SecurityGroupCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect SecurityGroups from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("SecurityGroupCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting SecurityGroups", e);
		}
	}
}
