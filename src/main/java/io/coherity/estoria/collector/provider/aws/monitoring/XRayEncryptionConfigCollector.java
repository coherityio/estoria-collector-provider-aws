package io.coherity.estoria.collector.provider.aws.monitoring;

import java.time.Instant;
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
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.xray.XRayClient;
import software.amazon.awssdk.services.xray.model.EncryptionConfig;
import software.amazon.awssdk.services.xray.model.GetEncryptionConfigResponse;

@Slf4j
public class XRayEncryptionConfigCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "XRayEncryptionConfig";


    public XRayEncryptionConfigCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "xray", "encryption", "config", "aws")).build());
    }

    @Override public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }
    @Override public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }
    @Override public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        XRayClient xRayClient = AwsClientFactory.getInstance().getXRayClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            GetEncryptionConfigResponse response = xRayClient.getEncryptionConfig();
            EncryptionConfig encryptionConfig = response.encryptionConfig();

            if (encryptionConfig == null)
            {
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

            String qualifiedName = ARNHelper.xRayEncryptionConfigArn(region, accountId);
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("keyId", encryptionConfig.keyId());
            attributes.put("status", encryptionConfig.statusAsString());
            attributes.put("type", encryptionConfig.typeAsString());
            attributes.put("accountId", accountId);
            attributes.put("region", region);
            attributes.put("arn", qualifiedName);

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id("default")
                    .qualifiedResourceName(qualifiedName)
                    .build())
                .entityType(ENTITY_TYPE)
                .name("default")
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(encryptionConfig)
                .collectedAt(Instant.now())
                .build();

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return List.of(entity); }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(Map.of("count", 1)).build();
                }
            };
        }
        catch (Exception e)
        {
            log.error("XRayEncryptionConfigCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting X-Ray encryption config", e);
        }
    }
}