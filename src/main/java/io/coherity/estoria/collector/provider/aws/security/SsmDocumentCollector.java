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
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.DocumentIdentifier;
import software.amazon.awssdk.services.ssm.model.DocumentKeyValuesFilter;
import software.amazon.awssdk.services.ssm.model.ListDocumentsRequest;
import software.amazon.awssdk.services.ssm.model.ListDocumentsResponse;
import software.amazon.awssdk.services.ssm.model.SsmException;

/**
 * Collects SSM Documents owned by this account (Self).
 */
@Slf4j
public class SsmDocumentCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SsmDocument";
    private static final int PAGE_SIZE = 50;

    private SsmClient ssmClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "ssm", "aws"))
            .build();

    public SsmDocumentCollector()
    {
        log.debug("SsmDocumentCollector created");
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
        log.debug("SsmDocumentCollector.collect called");

        if (this.ssmClient == null)
        {
            this.ssmClient = AwsClientFactory.getInstance().getSsmClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                ListDocumentsResponse response = this.ssmClient.listDocuments(
                    ListDocumentsRequest.builder()
                        .filters(DocumentKeyValuesFilter.builder()
                            .key("Owner")
                            .values("Self")
                            .build())
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (DocumentIdentifier doc : response.documentIdentifiers())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", doc.name());
                    attributes.put("owner", doc.owner());
                    attributes.put("documentVersion", doc.documentVersion());
                    attributes.put("documentType", doc.documentTypeAsString());
                    attributes.put("schemaVersion", doc.schemaVersion());
                    attributes.put("documentFormat", doc.documentFormatAsString());
                    attributes.put("targetType", doc.targetType());
                    attributes.put("platformTypes", doc.platformTypesAsStrings());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(doc.name())
                            .qualifiedResourceName(doc.name())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(doc.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(doc)
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
        catch (SsmException e)
        {
            log.error("SsmDocumentCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSM documents", e);
        }
        catch (Exception e)
        {
            log.error("SsmDocumentCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSM documents", e);
        }
    }
}
