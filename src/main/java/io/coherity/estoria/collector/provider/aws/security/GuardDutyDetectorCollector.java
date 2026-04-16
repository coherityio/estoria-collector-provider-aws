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
import software.amazon.awssdk.services.guardduty.GuardDutyClient;
import software.amazon.awssdk.services.guardduty.model.GetDetectorRequest;
import software.amazon.awssdk.services.guardduty.model.GetDetectorResponse;
import software.amazon.awssdk.services.guardduty.model.GuardDutyException;
import software.amazon.awssdk.services.guardduty.model.ListDetectorsRequest;
import software.amazon.awssdk.services.guardduty.model.ListDetectorsResponse;

/**
 * Collects Amazon GuardDuty detectors and their configuration.
 */
@Slf4j
public class GuardDutyDetectorCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "GuardDutyDetector";


    public GuardDutyDetectorCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "guardduty", "aws")).build());
        log.debug("GuardDutyDetectorCollector created");
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
        log.debug("GuardDutyDetectorCollector.collect called");

        GuardDutyClient guardDutyClient = AwsClientFactory.getInstance().getGuardDutyClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                ListDetectorsResponse listResponse = guardDutyClient.listDetectors(
                    ListDetectorsRequest.builder()
                        .maxResults(50)
                        .nextToken(nextToken)
                        .build());

                for (String detectorId : listResponse.detectorIds())
                {
                    GetDetectorResponse detector = guardDutyClient.getDetector(
                        GetDetectorRequest.builder()
                            .detectorId(detectorId)
                            .build());

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("detectorId", detectorId);
                    attributes.put("status", detector.statusAsString());
                    attributes.put("serviceRole", detector.serviceRole());
                    attributes.put("createdAt", detector.createdAt());
                    attributes.put("updatedAt", detector.updatedAt());
                    attributes.put("findingPublishingFrequency", detector.findingPublishingFrequencyAsString());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(detectorId)
                            .qualifiedResourceName(detectorId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(detectorId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(detector)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextToken = listResponse.nextToken();
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
        catch (GuardDutyException e)
        {
            log.error("GuardDutyDetectorCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect GuardDuty detectors", e);
        }
        catch (Exception e)
        {
            log.error("GuardDutyDetectorCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting GuardDuty detectors", e);
        }
    }
}
