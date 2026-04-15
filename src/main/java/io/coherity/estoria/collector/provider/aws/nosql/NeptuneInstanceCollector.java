package io.coherity.estoria.collector.provider.aws.nosql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.neptune.NeptuneClient;
import software.amazon.awssdk.services.neptune.model.DBInstance;
import software.amazon.awssdk.services.neptune.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.neptune.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.neptune.model.Filter;
import software.amazon.awssdk.services.neptune.model.NeptuneException;
import software.amazon.awssdk.services.neptune.model.Tag;

/**
 * Collects Neptune DB instances via the Neptune DescribeDBInstances API.
 */
@Slf4j
public class NeptuneInstanceCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "NeptuneInstance";

    private NeptuneClient neptuneClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "neptune", "graph", "aws"))
            .build();

    public NeptuneInstanceCollector()
    {
        log.debug("NeptuneInstanceCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo() { return this.collectorInfo; }

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
        log.debug("NeptuneInstanceCollector.collectEntities called");

        if (this.neptuneClient == null)
        {
            this.neptuneClient = AwsClientFactory.getInstance().getNeptuneClient(providerContext);
        }

        try
        {
            DescribeDbInstancesRequest.Builder requestBuilder = DescribeDbInstancesRequest.builder()
                .filters(List.of(Filter.builder().name("engine").values("neptune").build()));

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("NeptuneInstanceCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbInstancesResponse response = this.neptuneClient.describeDBInstances(requestBuilder.build());
            List<DBInstance> instances = response.dbInstances();
            String nextMarker = response.marker();

            log.debug("NeptuneInstanceCollector received {} instances, nextMarker={}",
                instances != null ? instances.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (instances != null)
            {
                for (DBInstance instance : instances)
                {
                    if (instance == null) continue;

                    String instanceIdentifier = instance.dbInstanceIdentifier();
                    String arn                = instance.dbInstanceArn();

                    Map<String, String> tags = new HashMap<>();

                    if (arn != null && !arn.isBlank())
                    {
                        tags = this.neptuneClient.listTagsForResource(r -> r.resourceName(arn))
                            .tagList()
                            .stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));
                    }                    
                    
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbInstanceIdentifier", instanceIdentifier);
                    attributes.put("dbInstanceArn", arn);
                    attributes.put("dbInstanceClass", instance.dbInstanceClass());
                    attributes.put("engine", instance.engine());
                    attributes.put("engineVersion", instance.engineVersion());
                    attributes.put("dbInstanceStatus", instance.dbInstanceStatus());
                    attributes.put("endpoint",
                        instance.endpoint() != null ? instance.endpoint().address() : null);
                    attributes.put("port",
                        instance.endpoint() != null ? instance.endpoint().port() : null);
                    attributes.put("availabilityZone", instance.availabilityZone());
                    attributes.put("dbClusterIdentifier", instance.dbClusterIdentifier());
                    attributes.put("dbSubnetGroupName",
                        instance.dbSubnetGroup() != null ? instance.dbSubnetGroup().dbSubnetGroupName() : null);
                    attributes.put("multiAZ", instance.multiAZ());
                    attributes.put("storageEncrypted", instance.storageEncrypted());
                    attributes.put("kmsKeyId", instance.kmsKeyId());
                    attributes.put("publiclyAccessible", instance.publiclyAccessible());
                    attributes.put("iamDatabaseAuthenticationEnabled", instance.iamDatabaseAuthenticationEnabled());
                    attributes.put("deletionProtection", instance.deletionProtection());
                    attributes.put("instanceCreateTime",
                        instance.instanceCreateTime() != null ? instance.instanceCreateTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(instanceIdentifier)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(instance)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextMarker = nextMarker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextMarker).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (NeptuneException e)
        {
            log.error("NeptuneInstanceCollector Neptune error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Neptune instances", e);
        }
        catch (Exception e)
        {
            log.error("NeptuneInstanceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Neptune instances", e);
        }
    }
}
