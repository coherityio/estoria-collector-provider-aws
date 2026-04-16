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
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.redshift.model.DescribeClusterSnapshotsRequest;
import software.amazon.awssdk.services.redshift.model.DescribeClusterSnapshotsResponse;
import software.amazon.awssdk.services.redshift.model.RedshiftException;
import software.amazon.awssdk.services.redshift.model.Snapshot;
import software.amazon.awssdk.services.redshift.model.Tag;

/**
 * Collects Redshift cluster snapshots via the Redshift DescribeClusterSnapshots API.
 */
@Slf4j
public class RedshiftSnapshotCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RedshiftSnapshot";


    public RedshiftSnapshotCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "redshift", "snapshot", "aws")).build());
        log.debug("RedshiftSnapshotCollector created");
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
        log.debug("RedshiftSnapshotCollector.collectEntities called");

        RedshiftClient redshiftClient = AwsClientFactory.getInstance().getRedshiftClient(providerContext);

        try
        {
            DescribeClusterSnapshotsRequest.Builder requestBuilder = DescribeClusterSnapshotsRequest.builder()
                .ownerAccount(awsSessionContext.getCurrentAccountId());

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RedshiftSnapshotCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeClusterSnapshotsResponse response = redshiftClient.describeClusterSnapshots(requestBuilder.build());
            List<Snapshot> snapshots = response.snapshots();
            String nextMarker = response.marker();

            log.debug("RedshiftSnapshotCollector received {} snapshots, nextMarker={}",
                snapshots != null ? snapshots.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (snapshots != null)
            {
                for (Snapshot snapshot : snapshots)
                {
                    if (snapshot == null) continue;

                    String snapshotIdentifier = snapshot.snapshotIdentifier();
                    String clusterIdentifier  = snapshot.clusterIdentifier();

                    Map<String, String> tags = snapshot.tags() == null ? new HashMap<>()
                        : snapshot.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("snapshotIdentifier", snapshotIdentifier);
                    attributes.put("clusterIdentifier", clusterIdentifier);
                    attributes.put("snapshotType", snapshot.snapshotType());
                    attributes.put("status", snapshot.status());
                    attributes.put("nodeType", snapshot.nodeType());
                    attributes.put("numberOfNodes", snapshot.numberOfNodes());
                    attributes.put("dbName", snapshot.dbName());
                    attributes.put("port", snapshot.port());
                    attributes.put("availabilityZone", snapshot.availabilityZone());
                    attributes.put("masterUsername", snapshot.masterUsername());
                    attributes.put("encrypted", snapshot.encrypted());
                    attributes.put("kmsKeyId", snapshot.kmsKeyId());
                    attributes.put("ownerAccount", snapshot.ownerAccount());
                    attributes.put("totalBackupSizeInMegaBytes", snapshot.totalBackupSizeInMegaBytes());
                    attributes.put("snapshotCreateTime",
                        snapshot.snapshotCreateTime() != null ? snapshot.snapshotCreateTime().toString() : null);
                    attributes.put("clusterCreateTime",
                        snapshot.clusterCreateTime() != null ? snapshot.clusterCreateTime().toString() : null);
                    attributes.put("tags", tags);

                    // Construct snapshot ARN: arn:aws:redshift:region:account:snapshot:cluster/snapshotId
                    String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : "";
                    String accountId = awsSessionContext.getCurrentAccountId();
                    String arn = "arn:aws:redshift:" + region + ":" + accountId
                        + ":snapshot:" + clusterIdentifier + "/" + snapshotIdentifier;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(snapshotIdentifier)
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
        catch (RedshiftException e)
        {
            log.error("RedshiftSnapshotCollector Redshift error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Redshift snapshots", e);
        }
        catch (Exception e)
        {
            log.error("RedshiftSnapshotCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Redshift snapshots", e);
        }
    }
}
