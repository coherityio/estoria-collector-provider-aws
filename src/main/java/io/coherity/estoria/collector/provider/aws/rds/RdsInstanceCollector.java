package io.coherity.estoria.collector.provider.aws.rds;

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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;

/**
 * Collects RDS DB instances via the RDS DescribeDBInstances API.
 */
@Slf4j
public class RdsInstanceCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsInstance";

    private RdsClient rdsClient;

    public RdsInstanceCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "aws")).build());
        log.debug("RdsInstanceCollector created");
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
        log.debug("RdsInstanceCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeDbInstancesRequest.Builder requestBuilder = DescribeDbInstancesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsInstanceCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbInstancesResponse response = this.rdsClient.describeDBInstances(requestBuilder.build());
            List<DBInstance> instances = response.dbInstances();
            String nextMarker = response.marker();

            log.debug("RdsInstanceCollector received {} instances, nextMarker={}",
                instances != null ? instances.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (instances != null)
            {
                for (DBInstance instance : instances)
                {
                    if (instance == null) continue;

                    String dbInstanceIdentifier = instance.dbInstanceIdentifier();
                    String arn = instance.dbInstanceArn();

                    Map<String, String> tags = instance.tagList() == null ? new HashMap<>()
                        : instance.tagList().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", dbInstanceIdentifier);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbInstanceIdentifier", dbInstanceIdentifier);
                    attributes.put("dbInstanceArn", arn);
                    attributes.put("dbInstanceClass", instance.dbInstanceClass());
                    attributes.put("engine", instance.engine());
                    attributes.put("engineVersion", instance.engineVersion());
                    attributes.put("dbInstanceStatus", instance.dbInstanceStatus());
                    attributes.put("masterUsername", instance.masterUsername());
                    attributes.put("dbName", instance.dbName());
                    attributes.put("allocatedStorage", instance.allocatedStorage());
                    attributes.put("availabilityZone", instance.availabilityZone());
                    attributes.put("multiAZ", instance.multiAZ());
                    attributes.put("publiclyAccessible", instance.publiclyAccessible());
                    attributes.put("storageType", instance.storageType());
                    attributes.put("storageEncrypted", instance.storageEncrypted());
                    attributes.put("kmsKeyId", instance.kmsKeyId());
                    attributes.put("dbiResourceId", instance.dbiResourceId());
                    attributes.put("dbClusterIdentifier", instance.dbClusterIdentifier());
                    attributes.put("iamDatabaseAuthenticationEnabled", instance.iamDatabaseAuthenticationEnabled());
                    attributes.put("deletionProtection", instance.deletionProtection());
                    attributes.put("instanceCreateTime",
                        instance.instanceCreateTime() != null ? instance.instanceCreateTime().toString() : null);
                    attributes.put("latestRestorableTime",
                        instance.latestRestorableTime() != null ? instance.latestRestorableTime().toString() : null);
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : dbInstanceIdentifier)
                            .qualifiedResourceName(arn != null ? arn : dbInstanceIdentifier)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
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
        catch (RdsException e)
        {
            log.error("RdsInstanceCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS instances", e);
        }
        catch (Exception e)
        {
            log.error("RdsInstanceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS instances", e);
        }
    }
}
