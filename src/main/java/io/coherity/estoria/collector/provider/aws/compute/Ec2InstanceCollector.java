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
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 instances (state, type, metadata) via the EC2 DescribeInstances API.
 */
@Slf4j
public class Ec2InstanceCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "Ec2Instance";


    public Ec2InstanceCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("compute", "ec2", "instance", "aws")).build());
        log.debug("Ec2InstanceCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.VPC;
    }

    @Override
    public EntityCategory getEntityCategory()
    {
        return EntityCategory.RESOURCE;
    }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("Ec2InstanceCollector.collect called");

        Ec2Client ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            DescribeInstancesRequest.Builder requestBuilder = DescribeInstancesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2InstanceCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeInstancesResponse response = ec2Client.describeInstances(requestBuilder.build());
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Reservation reservation : response.reservations())
            {
                for (Instance instance : reservation.instances())
                {
                    if (instance == null) continue;

                    String instanceId = instance.instanceId();
                    String arn = ARNHelper.ec2InstanceArn(region, accountId, instanceId);

                    Map<String, String> tags = instance.tags() == null ? Map.of()
                        : instance.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("instanceId", instanceId);
                    attributes.put("instanceType", instance.instanceTypeAsString());
                    attributes.put("state", instance.state() != null ? instance.state().nameAsString() : null);
                    attributes.put("privateIpAddress", instance.privateIpAddress());
                    attributes.put("publicIpAddress", instance.publicIpAddress());
                    attributes.put("subnetId", instance.subnetId());
                    attributes.put("vpcId", instance.vpcId());
                    attributes.put("imageId", instance.imageId());
                    attributes.put("keyName", instance.keyName());
                    attributes.put("platform", instance.platformAsString());
                    attributes.put("architecture", instance.architectureAsString());
                    attributes.put("launchTime",
                        instance.launchTime() != null ? instance.launchTime().toString() : null);
                    attributes.put("iamInstanceProfileArn",
                        instance.iamInstanceProfile() != null ? instance.iamInstanceProfile().arn() : null);
                    attributes.put("availabilityZone",
                        instance.placement() != null ? instance.placement().availabilityZone() : null);
                    attributes.put("tags", tags);

                    String name = tags.getOrDefault("Name", instanceId);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(instanceId)
                            .qualifiedResourceName(arn)
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

            log.debug("Ec2InstanceCollector collected {} instances, nextToken={}", entities.size(), nextToken);

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
            log.error("Ec2InstanceCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 instances", e);
        }
        catch (Exception e)
        {
            log.error("Ec2InstanceCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 instances", e);
        }
    }

}
