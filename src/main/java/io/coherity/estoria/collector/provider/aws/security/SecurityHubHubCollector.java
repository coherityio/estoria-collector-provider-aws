package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
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
import software.amazon.awssdk.services.securityhub.model.DescribeHubRequest;
import software.amazon.awssdk.services.securityhub.model.DescribeHubResponse;
import software.amazon.awssdk.services.securityhub.model.SecurityHubException;

/**
 * Collects the AWS Security Hub hub configuration for the current account/region.
 * Emits a single entity if Security Hub is enabled.
 */
@Slf4j
public class SecurityHubHubCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SecurityHubHub";

    private SecurityHubClient securityHubClient;

    public SecurityHubHubCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "securityhub", "aws")).build());
        log.debug("SecurityHubHubCollector created");
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
        log.debug("SecurityHubHubCollector.collect called");

        if (this.securityHubClient == null)
        {
            this.securityHubClient = AwsClientFactory.getInstance().getSecurityHubClient(providerContext);
        }

        try
        {
            DescribeHubResponse response = this.securityHubClient.describeHub(
                DescribeHubRequest.builder().build());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("hubArn", response.hubArn());
            attributes.put("subscribedAt", response.subscribedAt());
            attributes.put("autoEnableControls", response.autoEnableControls());
            attributes.put("controlFindingGenerator", response.controlFindingGeneratorAsString());

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id(response.hubArn())
                    .qualifiedResourceName(response.hubArn())
                    .build())
                .entityType(ENTITY_TYPE)
                .name(response.hubArn())
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(response)
                .collectedAt(Instant.now())
                .build();

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", 1);

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return List.of(entity); }
                @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (SecurityHubException e)
        {
            // Security Hub not enabled
            if ("InvalidAccessException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null)
                || "AccessDeniedException".equals(e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null))
            {
                log.info("Security Hub not enabled in this region, no hub to collect");
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
            log.error("SecurityHubHubCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Security Hub hub", e);
        }
        catch (Exception e)
        {
            log.error("SecurityHubHubCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Security Hub hub", e);
        }
    }
}
