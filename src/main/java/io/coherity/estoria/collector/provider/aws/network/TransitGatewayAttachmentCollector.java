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
import software.amazon.awssdk.services.ec2.model.DescribeTransitGatewayAttachmentsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeTransitGatewayAttachmentsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TransitGatewayAttachment;

/**
 * Transit Gateway Attachment collector for AWS backed by the EC2 DescribeTransitGatewayAttachments API.
 */
@Slf4j
public class TransitGatewayAttachmentCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "TransitGatewayAttachment";


	public TransitGatewayAttachmentCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of("TransitGateway"), Set.of("networking", "transit-gateway", "aws")).build());
		log.debug("TransitGatewayAttachmentCollector.TransitGatewayAttachmentCollector creating TransitGatewayAttachmentCollector");
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
		log.debug("TransitGatewayAttachmentCollector.collectEntities called with request: {}", collectorRequestParams);

		Ec2Client ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);

		try
		{
			Region region = awsSessionContext.getRegion();

			log.debug("TransitGatewayAttachmentCollector.collect using region: {}", region);

			DescribeTransitGatewayAttachmentsRequest.Builder requestBuilder = DescribeTransitGatewayAttachmentsRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0)
			{
				requestBuilder.maxResults(pageSize);
			}

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("TransitGatewayAttachmentCollector.collect resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			DescribeTransitGatewayAttachmentsRequest describeRequest = requestBuilder.build();
			log.debug("TransitGatewayAttachmentCollector.collect calling DescribeTransitGatewayAttachments with maxResults={} nextToken={}",
				describeRequest.maxResults(), describeRequest.nextToken());
			DescribeTransitGatewayAttachmentsResponse response = ec2Client.describeTransitGatewayAttachments(describeRequest);
			List<TransitGatewayAttachment> attachments = response.transitGatewayAttachments();
			String nextToken = response.nextToken();

			log.debug("TransitGatewayAttachmentCollector.collect received {} attachments, nextToken={}",
				attachments != null ? attachments.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (attachments != null)
			{
				for (TransitGatewayAttachment attachment : attachments)
				{
					if (attachment == null)
					{
						continue;
					}

					String attachmentId = attachment.transitGatewayAttachmentId();
					String ownerId = attachment.creationTime() != null ? null : null; // No ownerId field; use resourceOwnerId
					String resourceOwnerId = attachment.resourceOwnerId();
					String regionName = region != null ? region.id() : null;
					String arn = ARNHelper.ec2TransitGatewayAttachmentArn(regionName, resourceOwnerId, attachmentId);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("transitGatewayAttachmentId", attachmentId);
					attributes.put("transitGatewayId", attachment.transitGatewayId());
					attributes.put("resourceType", attachment.resourceTypeAsString());
					attributes.put("resourceId", attachment.resourceId());
					attributes.put("resourceOwnerId", resourceOwnerId);
					attributes.put("state", attachment.stateAsString());

					Map<String, String> tags = attachment.tags() == null ? Map.of()
						: attachment.tags().stream()
							.collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
					attributes.put("tags", tags);

					String name = tags.getOrDefault("Name", attachmentId);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(attachmentId)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(name)
						.collectorContext(collectorContext)
						.attributes(attributes)
						.rawPayload(attachment)
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
			log.error("TransitGatewayAttachmentCollector.collect EC2 error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect TransitGatewayAttachments from AWS EC2", e);
		}
		catch (Exception e)
		{
			log.error("TransitGatewayAttachmentCollector.collect unexpected error", e);
			throw new CollectorException("Unexpected error while collecting TransitGatewayAttachments", e);
		}
	}
}
