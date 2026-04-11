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
import software.amazon.awssdk.services.kms.model.DescribeKeyRequest;
import software.amazon.awssdk.services.kms.model.DescribeKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyListEntry;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KmsException;
import software.amazon.awssdk.services.kms.model.ListKeysRequest;
import software.amazon.awssdk.services.kms.model.ListKeysResponse;

/**
 * Collects AWS KMS Customer Managed Keys (CMKs) and their metadata.
 */
@Slf4j
public class KmsKeyCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "KmsKey";
    private static final int PAGE_SIZE = 100;

    private KmsClient kmsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "kms", "aws"))
            .build();

    public KmsKeyCollector()
    {
        log.debug("KmsKeyCollector created");
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
        log.debug("KmsKeyCollector.collect called");

        if (this.kmsClient == null)
        {
            this.kmsClient = AwsClientFactory.getInstance().getKmsClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String marker = null;

            do
            {
                ListKeysResponse listResponse = this.kmsClient.listKeys(
                    ListKeysRequest.builder()
                        .limit(PAGE_SIZE)
                        .marker(marker)
                        .build());

                for (KeyListEntry keyEntry : listResponse.keys())
                {
                    try
                    {
                        DescribeKeyResponse describeResponse = this.kmsClient.describeKey(
                            DescribeKeyRequest.builder()
                                .keyId(keyEntry.keyId())
                                .build());
                        KeyMetadata km = describeResponse.keyMetadata();

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("keyId", km.keyId());
                        attributes.put("arn", km.arn());
                        attributes.put("description", km.description());
                        attributes.put("enabled", km.enabled());
                        attributes.put("keyState", km.keyStateAsString());
                        attributes.put("keyUsage", km.keyUsageAsString());
                        attributes.put("origin", km.originAsString());
                        attributes.put("keyManager", km.keyManagerAsString());
                        attributes.put("keySpec", km.keySpecAsString());
                        attributes.put("multiRegion", km.multiRegion());
                        attributes.put("creationDate", km.creationDate() != null ? km.creationDate().toString() : null);
                        attributes.put("deletionDate", km.deletionDate() != null ? km.deletionDate().toString() : null);

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(km.arn())
                                .qualifiedResourceName(km.arn())
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(km.keyId())
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(km)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);
                    }
                    catch (KmsException e)
                    {
                        log.warn("Skipping KMS key {}: {}", keyEntry.keyId(),
                            e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage());
                    }
                }

                marker = listResponse.truncated() ? listResponse.nextMarker() : null;
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
            log.error("KmsKeyCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect KMS keys", e);
        }
        catch (Exception e)
        {
            log.error("KmsKeyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting KMS keys", e);
        }
    }
}
