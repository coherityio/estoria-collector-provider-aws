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
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVpcsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.Vpc;

/**
 * VPC collector for AWS backed by the EC2 DescribeVpcs API.
 */
@Slf4j
public class VpcCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "Vpc";

	private Ec2Client ec2Client;

	public VpcCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("networking", "vpc", "aws")).build());
		log.debug("VpcCollector.VpcCollector creating VpcCollector");
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
		log.debug("VpcCollector.collectEntities called with request: {}", collectorRequestParams);

		if(this.ec2Client == null)
		{
			this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
		}

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("VpcCollector.collect using region: {}", region);

			//Ec2Client ec2 = null; //AwsClientFactory.getInstance().getEc2Client(collectorContext);

			DescribeVpcsRequest.Builder requestBuilder = DescribeVpcsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("VpcCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeVpcsRequest describeRequest = requestBuilder.build();
			log.debug("VpcCollector.collect calling DescribeVpcs with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeVpcsResponse response = this.ec2Client.describeVpcs(describeRequest);
			List<Vpc> vpcs = response.vpcs();
			String nextToken = response.nextToken();

			log.debug("VpcCollector.collect received {} VPCs, nextToken={}",
				vpcs != null ? vpcs.size() : 0,
				nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (vpcs != null)
			{
				for (Vpc vpc : vpcs)
				{
					if (vpc == null)
					{
						continue;
					}

					String vpcId = vpc.vpcId();
					String ownerId = vpc.ownerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2VpcArn(regionName, ownerId, vpcId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("vpcId", vpcId);
					attributes.put("cidrBlock", vpc.cidrBlock());
					attributes.put("state", vpc.stateAsString());
					attributes.put("isDefault", vpc.isDefault());
					attributes.put("ownerId", ownerId);

					Map<String, String> tags = vpc.tags() == null ? Map.of()
						: vpc.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", vpcId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(vpcId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(vpc)
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
			log.error("VpcCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect VPCs from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("VpcCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting VPCs", e);
		}
	}
}
