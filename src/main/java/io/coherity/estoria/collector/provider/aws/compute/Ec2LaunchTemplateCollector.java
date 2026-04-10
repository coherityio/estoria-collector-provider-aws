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
import software.amazon.awssdk.services.ec2.model.DescribeLaunchTemplatesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeLaunchTemplatesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.LaunchTemplate;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 launch templates via the EC2 DescribeLaunchTemplates API.
 */
@Slf4j
public class Ec2LaunchTemplateCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "Ec2LaunchTemplate";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "launch-template", "aws"))
            .build();

    public Ec2LaunchTemplateCollector()
    {
        log.debug("Ec2LaunchTemplateCollector created");
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
        log.debug("Ec2LaunchTemplateCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = resolveRegion(providerContext);
            String accountId = resolveAccountId(providerContext);

            DescribeLaunchTemplatesRequest.Builder requestBuilder = DescribeLaunchTemplatesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2LaunchTemplateCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeLaunchTemplatesResponse response = this.ec2Client.describeLaunchTemplates(requestBuilder.build());
            List<LaunchTemplate> templates = response.launchTemplates();
            String nextToken = response.nextToken();

            log.debug("Ec2LaunchTemplateCollector received {} launch templates, nextToken={}",
                templates != null ? templates.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (templates != null)
            {
                for (LaunchTemplate template : templates)
                {
                    if (template == null) continue;

                    String templateId = template.launchTemplateId();
                    String arn = ARNHelper.ec2LaunchTemplateArn(region, accountId, templateId);

                    Map<String, String> tags = template.tags() == null ? Map.of()
                        : template.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("launchTemplateId", templateId);
                    attributes.put("launchTemplateName", template.launchTemplateName());
                    attributes.put("defaultVersionNumber", template.defaultVersionNumber());
                    attributes.put("latestVersionNumber", template.latestVersionNumber());
                    attributes.put("createdBy", template.createdBy());
                    attributes.put("createTime",
                        template.createTime() != null ? template.createTime().toString() : null);
                    attributes.put("tags", tags);

                    String name = template.launchTemplateName() != null
                        ? template.launchTemplateName() : templateId;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(templateId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(template)
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
            log.error("Ec2LaunchTemplateCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 launch templates", e);
        }
        catch (Exception e)
        {
            log.error("Ec2LaunchTemplateCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 launch templates", e);
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
