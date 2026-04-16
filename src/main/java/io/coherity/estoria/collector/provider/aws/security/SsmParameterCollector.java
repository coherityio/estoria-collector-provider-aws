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
import software.amazon.awssdk.services.ssm.model.DescribeParametersRequest;
import software.amazon.awssdk.services.ssm.model.DescribeParametersResponse;
import software.amazon.awssdk.services.ssm.model.ParameterMetadata;
import software.amazon.awssdk.services.ssm.model.SsmException;

/**
 * Collects AWS SSM Parameter Store parameter metadata (not values).
 */
@Slf4j
public class SsmParameterCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SsmParameter";
    private static final int PAGE_SIZE = 50;


    public SsmParameterCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "ssm", "aws")).build());
        log.debug("SsmParameterCollector created");
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
        log.debug("SsmParameterCollector.collect called");

        SsmClient ssmClient = AwsClientFactory.getInstance().getSsmClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                DescribeParametersResponse response = ssmClient.describeParameters(
                    DescribeParametersRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (ParameterMetadata param : response.parameters())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", param.name());
                    attributes.put("type", param.typeAsString());
                    attributes.put("keyId", param.keyId());
                    attributes.put("lastModifiedDate", param.lastModifiedDate() != null ? param.lastModifiedDate().toString() : null);
                    attributes.put("lastModifiedUser", param.lastModifiedUser());
                    attributes.put("description", param.description());
                    attributes.put("version", param.version());
                    attributes.put("tier", param.tierAsString());
                    attributes.put("dataType", param.dataType());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(param.name())
                            .qualifiedResourceName(param.name())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(param.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(param)
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
            log.error("SsmParameterCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSM parameters", e);
        }
        catch (Exception e)
        {
            log.error("SsmParameterCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSM parameters", e);
        }
    }
}
