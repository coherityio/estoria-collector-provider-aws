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
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.Association;
import software.amazon.awssdk.services.ssm.model.ListAssociationsRequest;
import software.amazon.awssdk.services.ssm.model.ListAssociationsResponse;
import software.amazon.awssdk.services.ssm.model.SsmException;

/**
 * Collects SSM State Manager associations (document-to-target bindings).
 */
@Slf4j
public class SsmAssociationCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SsmAssociation";
    private static final int PAGE_SIZE = 50;

    private SsmClient ssmClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "ssm", "aws"))
            .build();

    public SsmAssociationCollector()
    {
        log.debug("SsmAssociationCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("SsmAssociationCollector.collect called");

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
                ListAssociationsResponse response = this.ssmClient.listAssociations(
                    ListAssociationsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (Association association : response.associations())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("associationId", association.associationId());
                    attributes.put("associationName", association.associationName());
                    attributes.put("associationVersion", association.associationVersion());
                    attributes.put("instanceId", association.instanceId());
                    attributes.put("name", association.name());
                    attributes.put("documentVersion", association.documentVersion());
                    attributes.put("scheduleExpression", association.scheduleExpression());
                    attributes.put("lastExecutionDate", association.lastExecutionDate() != null ? association.lastExecutionDate().toString() : null);
                    attributes.put("overview", association.overview() != null ? association.overview().status() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(association.associationId())
                            .qualifiedResourceName(association.associationId())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(association.associationId())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(association)
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
            log.error("SsmAssociationCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSM associations", e);
        }
        catch (Exception e)
        {
            log.error("SsmAssociationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSM associations", e);
        }
    }
}
