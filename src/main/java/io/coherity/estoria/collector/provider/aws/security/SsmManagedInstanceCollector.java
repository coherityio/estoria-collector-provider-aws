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
import software.amazon.awssdk.services.ssm.model.DescribeInstanceInformationRequest;
import software.amazon.awssdk.services.ssm.model.DescribeInstanceInformationResponse;
import software.amazon.awssdk.services.ssm.model.InstanceInformation;
import software.amazon.awssdk.services.ssm.model.SsmException;

/**
 * Collects SSM-managed instances (EC2 and on-premises) registered with SSM.
 */
@Slf4j
public class SsmManagedInstanceCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "SsmManagedInstance";
    private static final int PAGE_SIZE = 50;

    private SsmClient ssmClient;

    public SsmManagedInstanceCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "ssm", "aws")).build());
        log.debug("SsmManagedInstanceCollector created");
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
        log.debug("SsmManagedInstanceCollector.collect called");

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
                DescribeInstanceInformationResponse response = this.ssmClient.describeInstanceInformation(
                    DescribeInstanceInformationRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (InstanceInformation instance : response.instanceInformationList())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("instanceId", instance.instanceId());
                    attributes.put("pingStatus", instance.pingStatusAsString());
                    attributes.put("lastPingDateTime", instance.lastPingDateTime() != null ? instance.lastPingDateTime().toString() : null);
                    attributes.put("agentVersion", instance.agentVersion());
                    attributes.put("isLatestVersion", instance.isLatestVersion());
                    attributes.put("platformType", instance.platformTypeAsString());
                    attributes.put("platformName", instance.platformName());
                    attributes.put("platformVersion", instance.platformVersion());
                    attributes.put("activationId", instance.activationId());
                    attributes.put("iamRole", instance.iamRole());
                    attributes.put("resourceType", instance.resourceTypeAsString());
                    attributes.put("name", instance.name());
                    attributes.put("ipAddress", instance.ipAddress());
                    attributes.put("computerName", instance.computerName());
                    attributes.put("associationStatus", instance.associationStatus());
                    attributes.put("registrationDate", instance.registrationDate() != null ? instance.registrationDate().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(instance.instanceId())
                            .qualifiedResourceName(instance.instanceId())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(instance.instanceId())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(instance)
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
            log.error("SsmManagedInstanceCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect SSM managed instances", e);
        }
        catch (Exception e)
        {
            log.error("SsmManagedInstanceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting SSM managed instances", e);
        }
    }
}
