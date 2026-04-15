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
import software.amazon.awssdk.services.macie2.Macie2Client;
import software.amazon.awssdk.services.macie2.model.Finding;
import software.amazon.awssdk.services.macie2.model.GetFindingsRequest;
import software.amazon.awssdk.services.macie2.model.GetFindingsResponse;
import software.amazon.awssdk.services.macie2.model.ListFindingsRequest;
import software.amazon.awssdk.services.macie2.model.ListFindingsResponse;
import software.amazon.awssdk.services.macie2.model.Macie2Exception;
import software.amazon.awssdk.services.macie2.model.OrderBy;
import software.amazon.awssdk.services.macie2.model.SortCriteria;

/**
 * Collects Amazon Macie findings (security findings about S3 data).
 * Fetches finding IDs in batches, then retrieves full details.
 */
@Slf4j
public class MacieFindingCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "MacieFinding";
    private static final int PAGE_SIZE = 50;
    private static final int BATCH_SIZE = 25; // getFindings max

    private Macie2Client macie2Client;

    public MacieFindingCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "macie", "aws")).build());
        log.debug("MacieFindingCollector created");
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
        log.debug("MacieFindingCollector.collect called");

        if (this.macie2Client == null)
        {
            this.macie2Client = AwsClientFactory.getInstance().getMacie2Client(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // Step 1: collect all finding IDs
            List<String> findingIds = new ArrayList<>();
            String nextToken = null;
            do
            {
                ListFindingsResponse listResponse = this.macie2Client.listFindings(
                    ListFindingsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .sortCriteria(SortCriteria.builder()
                            .attributeName("updatedAt")
                            .orderBy(OrderBy.DESC)
                            .build())
                        .build());
                findingIds.addAll(listResponse.findingIds());
                nextToken = listResponse.nextToken();
            }
            while (nextToken != null);

            // Step 2: batch fetch details
            for (int i = 0; i < findingIds.size(); i += BATCH_SIZE)
            {
                List<String> batch = findingIds.subList(i, Math.min(i + BATCH_SIZE, findingIds.size()));
                GetFindingsResponse getResponse = this.macie2Client.getFindings(
                    GetFindingsRequest.builder()
                        .findingIds(batch)
                        .build());

                for (Finding finding : getResponse.findings())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("findingId", finding.id());
                    attributes.put("accountId", finding.accountId());
                    attributes.put("arn", finding.id());
                    attributes.put("type", finding.typeAsString());
                    attributes.put("title", finding.title());
                    attributes.put("description", finding.description());
                    attributes.put("severity", finding.severity() != null ? finding.severity().descriptionAsString() : null);
                    attributes.put("region", finding.region());
                    attributes.put("createdAt", finding.createdAt() != null ? finding.createdAt().toString() : null);
                    attributes.put("updatedAt", finding.updatedAt() != null ? finding.updatedAt().toString() : null);
                    attributes.put("archived", finding.archived());
                    attributes.put("count", finding.count());

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
            }

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
        catch (Macie2Exception e)
        {
            // Macie not enabled in this region/account
            if ("AccessDeniedException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
            {
                log.info("Macie2 not enabled or access denied, no findings to collect");
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
            log.error("MacieFindingCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Macie findings", e);
        }
        catch (Exception e)
        {
            log.error("MacieFindingCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Macie findings", e);
        }
    }
}
