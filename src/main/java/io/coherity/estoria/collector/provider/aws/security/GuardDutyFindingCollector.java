package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.guardduty.GuardDutyClient;
import software.amazon.awssdk.services.guardduty.model.Finding;
import software.amazon.awssdk.services.guardduty.model.GetFindingsRequest;
import software.amazon.awssdk.services.guardduty.model.GetFindingsResponse;
import software.amazon.awssdk.services.guardduty.model.GuardDutyException;
import software.amazon.awssdk.services.guardduty.model.ListDetectorsRequest;
import software.amazon.awssdk.services.guardduty.model.ListDetectorsResponse;
import software.amazon.awssdk.services.guardduty.model.ListFindingsRequest;
import software.amazon.awssdk.services.guardduty.model.ListFindingsResponse;

/**
 * Collects Amazon GuardDuty findings for all detectors in the current region.
 * Fetches finding IDs per detector, then retrieves up to 50 findings at a time.
 */
@Slf4j
public class GuardDutyFindingCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "GuardDutyFinding";
    private static final int PAGE_SIZE = 50;
    private static final int BATCH_SIZE = 50; // getFindings max

    private GuardDutyClient guardDutyClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "guardduty", "aws"))
            .build();

    public GuardDutyFindingCollector()
    {
        log.debug("GuardDutyFindingCollector created");
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
        log.debug("GuardDutyFindingCollector.collect called");

        if (this.guardDutyClient == null)
        {
            this.guardDutyClient = AwsClientFactory.getInstance().getGuardDutyClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // List all detectors
            List<String> detectorIds = new ArrayList<>();
            String detectorNextToken = null;
            do
            {
                ListDetectorsResponse detectorResponse = this.guardDutyClient.listDetectors(
                    ListDetectorsRequest.builder()
                        .maxResults(50)
                        .nextToken(detectorNextToken)
                        .build());
                detectorIds.addAll(detectorResponse.detectorIds());
                detectorNextToken = detectorResponse.nextToken();
            }
            while (detectorNextToken != null);

            // For each detector, collect findings
            for (String detectorId : detectorIds)
            {
                List<String> findingIds = new ArrayList<>();
                String findingNextToken = null;
                do
                {
                    ListFindingsResponse listResponse = this.guardDutyClient.listFindings(
                        ListFindingsRequest.builder()
                            .detectorId(detectorId)
                            .maxResults(PAGE_SIZE)
                            .nextToken(findingNextToken)
                            .build());
                    findingIds.addAll(listResponse.findingIds());
                    findingNextToken = listResponse.nextToken();
                }
                while (findingNextToken != null);

                // Batch get findings
                for (int i = 0; i < findingIds.size(); i += BATCH_SIZE)
                {
                    List<String> batch = findingIds.subList(i, Math.min(i + BATCH_SIZE, findingIds.size()));
                    GetFindingsResponse getResponse = this.guardDutyClient.getFindings(
                        GetFindingsRequest.builder()
                            .detectorId(detectorId)
                            .findingIds(batch)
                            .build());

                    for (Finding finding : getResponse.findings())
                    {
                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("findingId", finding.id());
                        attributes.put("detectorId", detectorId);
                        attributes.put("arn", finding.arn());
                        attributes.put("accountId", finding.accountId());
                        attributes.put("region", finding.region());
                        attributes.put("type", finding.type());
                        attributes.put("title", finding.title());
                        attributes.put("description", finding.description());
                        attributes.put("severity", finding.severity());
                        attributes.put("createdAt", finding.createdAt());
                        attributes.put("updatedAt", finding.updatedAt());

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(finding.arn())
                                .qualifiedResourceName(finding.arn())
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
        catch (GuardDutyException e)
        {
            log.error("GuardDutyFindingCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect GuardDuty findings", e);
        }
        catch (Exception e)
        {
            log.error("GuardDutyFindingCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting GuardDuty findings", e);
        }
    }
}
