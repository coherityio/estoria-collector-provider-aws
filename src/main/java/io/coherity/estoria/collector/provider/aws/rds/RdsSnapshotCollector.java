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
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;

/**
 * Collects RDS DB snapshots (manual and automated) via the RDS DescribeDBSnapshots API.
 */
@Slf4j
public class RdsSnapshotCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "RdsSnapshot";

    private RdsClient rdsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("database", "rds", "snapshot", "aws"))
            .build();

    public RdsSnapshotCollector()
    {
        log.debug("RdsSnapshotCollector created");
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
        log.debug("RdsSnapshotCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            // SnapshotType "manual" and "automated" – omitting the filter returns all owned snapshots
            DescribeDbSnapshotsRequest.Builder requestBuilder = DescribeDbSnapshotsRequest.builder()
                .includeShared(false)
                .includePublic(false);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsSnapshotCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbSnapshotsResponse response = this.rdsClient.describeDBSnapshots(requestBuilder.build());
            List<DBSnapshot> snapshots = response.dbSnapshots();
            String nextMarker = response.marker();

            log.debug("RdsSnapshotCollector received {} snapshots, nextMarker={}",
                snapshots != null ? snapshots.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (snapshots != null)
            {
                for (DBSnapshot snapshot : snapshots)
                {
                    if (snapshot == null) continue;

                    String snapshotIdentifier = snapshot.dbSnapshotIdentifier();
                    String arn = snapshot.dbSnapshotArn();

                    Map<String, String> tags = snapshot.tagList() == null ? new HashMap<>()
                        : snapshot.tagList().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", snapshotIdentifier);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbSnapshotIdentifier", snapshotIdentifier);
                    attributes.put("dbSnapshotArn", arn);
                    attributes.put("dbInstanceIdentifier", snapshot.dbInstanceIdentifier());
                    attributes.put("snapshotType", snapshot.snapshotType());
                    attributes.put("engine", snapshot.engine());
                    attributes.put("engineVersion", snapshot.engineVersion());
                    attributes.put("status", snapshot.status());
                    attributes.put("port", snapshot.port());
                    attributes.put("availabilityZone", snapshot.availabilityZone());
                    attributes.put("allocatedStorage", snapshot.allocatedStorage());
                    attributes.put("storageType", snapshot.storageType());
                    attributes.put("encrypted", snapshot.encrypted());
                    attributes.put("kmsKeyId", snapshot.kmsKeyId());
                    attributes.put("dbiResourceId", snapshot.dbiResourceId());
                    attributes.put("iamDatabaseAuthenticationEnabled", snapshot.iamDatabaseAuthenticationEnabled());
                    attributes.put("snapshotCreateTime",
                        snapshot.snapshotCreateTime() != null ? snapshot.snapshotCreateTime().toString() : null);
                    attributes.put("instanceCreateTime",
                        snapshot.instanceCreateTime() != null ? snapshot.instanceCreateTime().toString() : null);
                    attributes.put("percentProgress", snapshot.percentProgress());
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : snapshotIdentifier)
                            .qualifiedResourceName(arn != null ? arn : snapshotIdentifier)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(snapshot)
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
            log.error("RdsSnapshotCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS snapshots", e);
        }
        catch (Exception e)
        {
            log.error("RdsSnapshotCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS snapshots", e);
        }
    }
}
