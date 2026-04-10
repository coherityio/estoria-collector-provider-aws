package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeImagesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Image;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects Amazon Machine Images (AMIs) owned by this account via DescribeImages.
 */
@Slf4j
public class Ec2ImageCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "Ec2Image";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "ami", "image", "aws"))
            .build();

    public Ec2ImageCollector()
    {
        log.debug("Ec2ImageCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("Ec2ImageCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = resolveRegion(providerContext);
            String accountId = resolveAccountId(providerContext);

            DescribeImagesRequest.Builder requestBuilder = DescribeImagesRequest.builder()
                // Restrict to images owned by this account
                .owners("self");

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2ImageCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            DescribeImagesResponse response = this.ec2Client.describeImages(requestBuilder.build());
            List<Image> images = response.images();
            String nextToken = response.nextToken();

            log.debug("Ec2ImageCollector received {} images, nextToken={}", 
                images != null ? images.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (images != null)
            {
                for (Image image : images)
                {
                    if (image == null) continue;

                    String imageId = image.imageId();
                    String arn = ARNHelper.ec2ImageArn(region, accountId, imageId);

                    Map<String, String> tags = image.tags() == null ? Map.of()
                        : image.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("imageId", imageId);
                    attributes.put("name", image.name());
                    attributes.put("description", image.description());
                    attributes.put("state", image.stateAsString());
                    attributes.put("architecture", image.architectureAsString());
                    attributes.put("platform", image.platformAsString());
                    attributes.put("virtualizationType", image.virtualizationTypeAsString());
                    attributes.put("rootDeviceType", image.rootDeviceTypeAsString());
                    attributes.put("rootDeviceName", image.rootDeviceName());
                    attributes.put("ownerId", image.ownerId());
                    attributes.put("public", image.publicLaunchPermissions());
                    attributes.put("creationDate", image.creationDate());
                    attributes.put("tags", tags);

                    String name = image.name() != null ? image.name() : imageId;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(imageId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(image)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (Ec2Exception e)
        {
            log.error("Ec2ImageCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 images (AMIs)", e);
        }
        catch (Exception e)
        {
            log.error("Ec2ImageCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 images", e);
        }
    }

    private static String resolveRegion(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("region");
            if (v != null) return v.toString();
        }
        return null;
    }

    private static String resolveAccountId(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("accountId");
            if (v != null) return v.toString();
        }
        return null;
    }
}
