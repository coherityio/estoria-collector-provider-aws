package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.ec2.model.DescribeKeyPairsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeKeyPairsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.KeyPairInfo;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.util.stream.Collectors;

/**
 * Collects EC2 SSH key pairs via the EC2 DescribeKeyPairs API.
 * Note: DescribeKeyPairs does not support pagination; it returns all results.
 */
@Slf4j
public class Ec2KeyPairCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "Ec2KeyPair";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "key-pair", "aws"))
            .build();

    public Ec2KeyPairCollector()
    {
        log.debug("Ec2KeyPairCollector created");
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
        log.debug("Ec2KeyPairCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = resolveRegion(providerContext);
            String accountId = resolveAccountId(providerContext);

            // DescribeKeyPairs does not support pagination
            DescribeKeyPairsResponse response = this.ec2Client.describeKeyPairs(
                DescribeKeyPairsRequest.builder().includePublicKey(true).build());
            List<KeyPairInfo> keyPairs = response.keyPairs();

            log.debug("Ec2KeyPairCollector received {} key pairs", keyPairs != null ? keyPairs.size() : 0);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (keyPairs != null)
            {
                for (KeyPairInfo kp : keyPairs)
                {
                    if (kp == null) continue;

                    String keyPairId = kp.keyPairId();
                    String arn = ARNHelper.ec2KeyPairArn(region, accountId, keyPairId);

                    Map<String, String> tags = kp.tags() == null ? Map.of()
                        : kp.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("keyPairId", keyPairId);
                    attributes.put("keyName", kp.keyName());
                    attributes.put("keyType", kp.keyTypeAsString());
                    attributes.put("keyFingerprint", kp.keyFingerprint());
                    attributes.put("publicKey", kp.publicKey());
                    attributes.put("createTime",
                        kp.createTime() != null ? kp.createTime().toString() : null);
                    attributes.put("tags", tags);

                    String name = kp.keyName() != null ? kp.keyName() : keyPairId;

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(keyPairId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(kp)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (Ec2Exception e)
        {
            log.error("Ec2KeyPairCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 key pairs", e);
        }
        catch (Exception e)
        {
            log.error("Ec2KeyPairCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 key pairs", e);
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
