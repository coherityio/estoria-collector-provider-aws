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
import software.amazon.awssdk.services.rds.model.DBClusterSnapshot;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterSnapshotsResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;

/**
 * Collects RDS cluster snapshots via the RDS DescribeDBClusterSnapshots API.
 */
@Slf4j
public class RdsClusterSnapshotCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsClusterSnapshot";

    private RdsClient rdsClient;

    public RdsClusterSnapshotCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "aurora", "snapshot", "aws")).build());
        log.debug("RdsClusterSnapshotCollector created");
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
        log.debug("RdsClusterSnapshotCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeDbClusterSnapshotsRequest.Builder requestBuilder =
                DescribeDbClusterSnapshotsRequest.builder()
                    .includeShared(false)
                    .includePublic(false);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsClusterSnapshotCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbClusterSnapshotsResponse response =
                this.rdsClient.describeDBClusterSnapshots(requestBuilder.build());
            List<DBClusterSnapshot> snapshots = response.dbClusterSnapshots();
            String nextMarker = response.marker();

            log.debug("RdsClusterSnapshotCollector received {} cluster snapshots, nextMarker={}",
                snapshots != null ? snapshots.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (snapshots != null)
            {
                for (DBClusterSnapshot snapshot : snapshots)
                {
                    if (snapshot == null) continue;

                    String snapshotIdentifier = snapshot.dbClusterSnapshotIdentifier();
                    String arn = snapshot.dbClusterSnapshotArn();

                    Map<String, String> tags = snapshot.tagList() == null ? new HashMap<>()
                        : snapshot.tagList().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    String name = tags.getOrDefault("Name", snapshotIdentifier);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbClusterSnapshotIdentifier", snapshotIdentifier);
                    attributes.put("dbClusterSnapshotArn", arn);
                    attributes.put("dbClusterIdentifier", snapshot.dbClusterIdentifier());
                    attributes.put("snapshotType", snapshot.snapshotType());
                    attributes.put("engine", snapshot.engine());
                    attributes.put("engineVersion", snapshot.engineVersion());
                    attributes.put("status", snapshot.status());
                    attributes.put("port", snapshot.port());
                    attributes.put("availabilityZones", snapshot.availabilityZones());
                    attributes.put("allocatedStorage", snapshot.allocatedStorage());
                    attributes.put("storageEncrypted", snapshot.storageEncrypted());
                    attributes.put("kmsKeyId", snapshot.kmsKeyId());
                    attributes.put("iamDatabaseAuthenticationEnabled", snapshot.iamDatabaseAuthenticationEnabled());
                    attributes.put("percentProgress", snapshot.percentProgress());
                    attributes.put("masterUsername", snapshot.masterUsername());
                    attributes.put("snapshotCreateTime",
                        snapshot.snapshotCreateTime() != null ? snapshot.snapshotCreateTime().toString() : null);
                    attributes.put("clusterCreateTime",
                        snapshot.clusterCreateTime() != null ? snapshot.clusterCreateTime().toString() : null);
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
            log.error("RdsClusterSnapshotCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS cluster snapshots", e);
        }
        catch (Exception e)
        {
            log.error("RdsClusterSnapshotCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS cluster snapshots", e);
        }
    }
}
