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
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.DescribeOrganizationRequest;
import software.amazon.awssdk.services.organizations.model.DescribeOrganizationResponse;
import software.amazon.awssdk.services.organizations.model.Organization;
import software.amazon.awssdk.services.organizations.model.OrganizationsException;

/**
 * Collects the AWS Organization root configuration via Organizations DescribeOrganization API.
 * Emits a single entity per management account.
 */
@Slf4j
public class OrganizationsOrganizationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "OrganizationsOrganization";

    private OrganizationsClient organizationsClient;

    public OrganizationsOrganizationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "organizations", "aws")).build());
        log.debug("OrganizationsOrganizationCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope() { return AccountScope.MANAGEMENT_ACCOUNT; }

    @Override
    public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ORGANIZATION; }

    @Override
    public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("OrganizationsOrganizationCollector.collect called");

        if (this.organizationsClient == null)
        {
            this.organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);
        }

        try
        {
            DescribeOrganizationResponse response = this.organizationsClient.describeOrganization(
                DescribeOrganizationRequest.builder().build());
            Organization org = response.organization();

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("organizationId", org.id());
            attributes.put("arn", org.arn());
            attributes.put("featureSet", org.featureSetAsString());
            attributes.put("masterAccountId", org.masterAccountId());
            attributes.put("masterAccountArn", org.masterAccountArn());
            attributes.put("masterAccountEmail", org.masterAccountEmail());

            CloudEntity entity = CloudEntity.builder()
                .entityIdentifier(EntityIdentifier.builder()
                    .id(org.arn())
                    .qualifiedResourceName(org.arn())
                    .build())
                .entityType(ENTITY_TYPE)
                .name(org.id())
                .collectorContext(collectorContext)
                .attributes(attributes)
                .rawPayload(org)
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
        catch (OrganizationsException e)
        {
            log.error("OrganizationsOrganizationCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Organizations organization", e);
        }
        catch (Exception e)
        {
            log.error("OrganizationsOrganizationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Organizations organization", e);
        }
    }
}
