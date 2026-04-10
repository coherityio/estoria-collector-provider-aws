package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.provider.aws.ARNHelper;
import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Snapshot;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EBS snapshots owned by this account via the EC2 DescribeSnapshots API.
 */
@Slf4j
public class Ec2SnapshotCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "Ec2Snapshot";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "ebs", "snapshot", "aws"))
            .build();

    public Ec2SnapshotCollector()
    {
        log.debug("Ec2SnapshotCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("Ec2SnapshotCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = resolveRegion(providerContext);
            String accountId = resolveAccountId(providerContext);

            DescribeSnapshotsRequest.Builder requestBuilder = DescribeSnapshotsRequest.builder()
                // Restrict to snapshots owned by this account
                .ownerIds("self");

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2SnapshotCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeSnapshotsResponse response = this.ec2Client.describeSnapshots(requestBuilder.build());
            List<Snapshot> snapshots = response.snapshots();
            String nextToken = response.nextToken();

            log.debug("Ec2SnapshotCollector received {} snapshots, nextToken={}",
                snapshots != null ? snapshots.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (snapshots != null)
            {
                for (Snapshot snapshot : snapshots)
                {
                    if (snapshot == null) continue;

                    String snapshotId = snapshot.snapshotId();
                    String arn = ARNHelper.ec2SnapshotArn(region, accountId, snapshotId);

                    Map<String, String> tags = snapshot.tags() == null ? Map.of()
                        : snapshot.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("snapshotId", snapshotId);
                    attributes.put("volumeId", snapshot.volumeId());
                    attributes.put("volumeSize", snapshot.volumeSize());
                    attributes.put("state", snapshot.stateAsString());
                    attributes.put("progress", snapshot.progress());
                    attributes.put("description", snapshot.description());
                    attributes.put("encrypted", snapshot.encrypted());
                    attributes.put("kmsKeyId", snapshot.kmsKeyId());
                    attributes.put("ownerId", snapshot.ownerId());
                    attributes.put("startTime",
                        snapshot.startTime() != null ? snapshot.startTime().toString() : null);
                    attributes.put("tags", tags);

                    String name = tags.getOrDefault("Name", snapshotId);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(snapshotId)
                            .qualifiedResourceName(arn)
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

            String finalNextToken = nextToken;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (Ec2Exception e)
        {
            log.error("Ec2SnapshotCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EBS snapshots", e);
        }
        catch (Exception e)
        {
            log.error("Ec2SnapshotCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EBS snapshots", e);
        }
    }

    private static String resolveRegion(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("region");
            if (v != null) return v.toString();
        }
        return null;
    }

    private static String resolveAccountId(ProviderContext ctx)
    {
        if (ctx != null && ctx.getAttributes() != null)
        {
            Object v = ctx.getAttributes().get("accountId");
            if (v != null) return v.toString();
        }
        return null;
    }
}
