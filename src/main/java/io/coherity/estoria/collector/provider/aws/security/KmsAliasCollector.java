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
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.AliasListEntry;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.kms.model.ListAliasesRequest;
import software.amazon.awssdk.services.kms.model.ListAliasesResponse;

/**
 * Collects all KMS key aliases in the account/region.
 */
@Slf4j
public class KmsAliasCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "KmsAlias";
    private static final int PAGE_SIZE = 100;


    public KmsAliasCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "kms", "aws")).build());
        log.debug("KmsAliasCollector created");
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
        log.debug("KmsAliasCollector.collect called");

        KmsClient kmsClient = AwsClientFactory.getInstance().getKmsClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String marker = null;

            do
            {
                ListAliasesResponse response = kmsClient.listAliases(
                    ListAliasesRequest.builder()
                        .limit(PAGE_SIZE)
                        .marker(marker)
                        .build());

                for (AliasListEntry alias : response.aliases())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("aliasName", alias.aliasName());
                    attributes.put("aliasArn", alias.aliasArn());
                    attributes.put("targetKeyId", alias.targetKeyId());
                    attributes.put("creationDate", alias.creationDate() != null ? alias.creationDate().toString() : null);
                    attributes.put("lastUpdatedDate", alias.lastUpdatedDate() != null ? alias.lastUpdatedDate().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(alias.aliasArn())
                            .qualifiedResourceName(alias.aliasArn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(alias.aliasName())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(alias)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                marker = response.truncated() ? response.nextMarker() : null;
            }
            while (marker != null);

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
        catch (KmsException e)
        {
            log.error("KmsAliasCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect KMS aliases", e);
        }
        catch (Exception e)
        {
            log.error("KmsAliasCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting KMS aliases", e);
        }
    }
}
