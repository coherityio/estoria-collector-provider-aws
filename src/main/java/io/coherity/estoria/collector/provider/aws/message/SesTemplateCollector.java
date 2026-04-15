package io.coherity.estoria.collector.provider.aws.message;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.sesv2.SesV2Client;
import software.amazon.awssdk.services.sesv2.model.EmailTemplateMetadata;
import software.amazon.awssdk.services.sesv2.model.ListEmailTemplatesRequest;
import software.amazon.awssdk.services.sesv2.model.ListEmailTemplatesResponse;
import software.amazon.awssdk.services.sesv2.model.SesV2Exception;

/**
 * Collects Amazon SES v2 email templates via the ListEmailTemplates API.
 */
@Slf4j
public class SesTemplateCollector extends AbstractAwsContextAwareCollector
{
	public static final String ENTITY_TYPE = "SesTemplate";

	private SesV2Client sesV2Client;

	public SesTemplateCollector()
	{
		super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("messaging", "ses", "email", "template", "aws")).build());
		log.debug("SesTemplateCollector created");
	}

	@Override
	public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }

	@Override
	public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }

	@Override
	public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

	@Override
	public CollectorCursor collectEntities(
		ProviderContext providerContext,
		AwsSessionContext awsSessionContext,
		CollectorContext collectorContext,
		CollectorRequestParams collectorRequestParams) throws CollectorException
	{
		log.debug("SesTemplateCollector.collectEntities called");

		if (this.sesV2Client == null)
		{
			this.sesV2Client = AwsClientFactory.getInstance().getSesV2Client(providerContext);
		}

		try
		{
			String accountId = awsSessionContext.getCurrentAccountId();
			String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

			ListEmailTemplatesRequest.Builder requestBuilder = ListEmailTemplatesRequest.builder();

			int pageSize = collectorRequestParams.getPageSize();
			if (pageSize > 0) requestBuilder.pageSize(pageSize);

			collectorRequestParams.getCursorToken().ifPresent(token -> {
				log.debug("SesTemplateCollector resuming from nextToken: {}", token);
				requestBuilder.nextToken(token);
			});

			ListEmailTemplatesResponse response = this.sesV2Client.listEmailTemplates(requestBuilder.build());
			List<EmailTemplateMetadata> templates = response.templatesMetadata();
			String                      nextToken = response.nextToken();

			log.debug("SesTemplateCollector received {} templates, nextToken={}",
				templates != null ? templates.size() : 0, nextToken);

			List<CloudEntity> entities = new ArrayList<>();
			Instant now = Instant.now();

			if (templates != null)
			{
				for (EmailTemplateMetadata template : templates)
				{
					if (template == null) continue;

					String templateName = template.templateName();
					if (templateName == null || templateName.isBlank()) continue;

					String arn = ARNHelper.sesTemplateArn(region, accountId, templateName);

					Map<String, Object> attributes = new HashMap<>();
					attributes.put("templateName", templateName);
					attributes.put("createdTimestamp",
						template.createdTimestamp() != null ? template.createdTimestamp().toString() : null);
					attributes.put("accountId", accountId);
					attributes.put("region",    region);

					CloudEntity entity = CloudEntity.builder()
						.entityIdentifier(EntityIdentifier.builder()
							.id(templateName)
							.qualifiedResourceName(arn)
							.build())
						.entityType(ENTITY_TYPE)
						.name(templateName)
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

			CursorMetadata metadata = CursorMetadata.builder().values(metadataValues).build();

			return new CollectorCursor()
			{
				@Override public List<CloudEntity> getEntities() { return entities; }
				@Override public Optional<String> getNextCursorToken()
				{
					return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
				}
				@Override public CursorMetadata getMetadata() { return metadata; }
			};
		}
		catch (SesV2Exception e)
		{
			log.error("SesTemplateCollector error: {}",
				e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
			throw new CollectorException("Failed to collect SES email templates", e);
		}
		catch (Exception e)
		{
			log.error("SesTemplateCollector unexpected error", e);
			throw new CollectorException("Unexpected error collecting SES email templates", e);
		}
	}
}
