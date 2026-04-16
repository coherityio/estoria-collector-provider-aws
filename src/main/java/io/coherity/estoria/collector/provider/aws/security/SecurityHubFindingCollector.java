package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.securityhub.SecurityHubClient;
import software.amazon.awssdk.services.securityhub.model.AwsSecurityFinding;
import software.amazon.awssdk.services.securityhub.model.GetFindingsRequest;
import software.amazon.awssdk.services.securityhub.model.GetFindingsResponse;
import software.amazon.awssdk.services.securityhub.model.SecurityHubException;
import software.amazon.awssdk.services.securityhub.model.SortCriterion;
import software.amazon.awssdk.services.securityhub.model.SortOrder;

/**
 * Collects AWS Security Hub findings.
 */
@Slf4j
public class SecurityHubFindingCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SecurityHubFinding";
    private static final int PAGE_SIZE = 100;


    public SecurityHubFindingCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "securityhub", "aws")).build());
        log.debug("SecurityHubFindingCollector created");
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
        log.debug("SecurityHubFindingCollector.collect called");

        SecurityHubClient securityHubClient = AwsClientFactory.getInstance().getSecurityHubClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                GetFindingsResponse response = securityHubClient.getFindings(
                    GetFindingsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .sortCriteria(SortCriterion.builder()
                            .field("UpdatedAt")
                            .sortOrder(SortOrder.DESC)
                            .build())
                        .build());

                for (AwsSecurityFinding finding : response.findings())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("findingId", finding.id());
                    attributes.put("arn", finding.id()); // Security Hub findings use ID as ARN-like identifier
                    attributes.put("awsAccountId", finding.awsAccountId());
                    attributes.put("schemaVersion", finding.schemaVersion());
                    attributes.put("generatorId", finding.generatorId());
                    attributes.put("productArn", finding.productArn());
                    attributes.put("title", finding.title());
                    attributes.put("description", finding.description());
                    attributes.put("severity", finding.severity() != null ? finding.severity().labelAsString() : null);
                    attributes.put("workflowStatus", finding.workflow() != null ? finding.workflow().statusAsString() : null);
                    attributes.put("recordState", finding.recordStateAsString());
                    attributes.put("createdAt", finding.createdAt());
                    attributes.put("updatedAt", finding.updatedAt());
                    attributes.put("complianceStatus", finding.compliance() != null ? finding.compliance().statusAsString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(finding.id())
                            .qualifiedResourceName(finding.id())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(finding.id())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(finding)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextToken = response.nextToken();
            }
            while (nextToken != null);

            final int count = entities.size();
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", count);

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (SecurityHubException e)
        {
            if ("InvalidAccessException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null)
                || "AccessDeniedException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
            {
                log.info("Security Hub not enabled, no findings to collect");
                return new CollectorCursor()
                {
                    @Override public List<CloudEntity> getEntities() { return List.of(); }
                    @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                    @Override public CursorMetadata getMetadata()
                    {
                        return CursorMetadata.builder().values(Map.of("count", 0)).build();
                    }
                };
            }
            log.error("SecurityHubFindingCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Security Hub findings", e);
        }
        catch (Exception e)
        {
            log.error("SecurityHubFindingCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Security Hub findings", e);
        }
    }
}
