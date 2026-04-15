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
import software.amazon.awssdk.services.ec2.model.DescribeHostsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeHostsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Host;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 dedicated hosts via the EC2 DescribeHosts API.
 */
@Slf4j
public class Ec2HostCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "Ec2Host";

    private Ec2Client ec2Client;

    public Ec2HostCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("compute", "ec2", "dedicated-host", "aws")).build());
        log.debug("Ec2HostCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.ACCOUNT_REGIONAL;
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
        log.debug("Ec2HostCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        try
        {
            String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
            String accountId = awsSessionContext.getCurrentAccountId();

            DescribeHostsRequest.Builder requestBuilder = DescribeHostsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("Ec2HostCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeHostsResponse response = this.ec2Client.describeHosts(requestBuilder.build());
            List<Host> hosts = response.hosts();
            String nextToken = response.nextToken();

            log.debug("Ec2HostCollector received {} hosts, nextToken={}", 
                hosts != null ? hosts.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (hosts != null)
            {
                for (Host host : hosts)
                {
                    if (host == null) continue;

                    String hostId = host.hostId();
                    String arn = ARNHelper.ec2HostArn(region, accountId, hostId);

                    Map<String, String> tags = host.tags() == null ? Map.of()
                        : host.tags().stream()
                            .collect(Collectors.toMap(Tag::key, Tag::value, (a, b) -> b));

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("hostId", hostId);
                    attributes.put("state", host.stateAsString());
                    attributes.put("availabilityZone", host.availabilityZone());
                    attributes.put("instanceType",
                        host.hostProperties() != null ? host.hostProperties().instanceType() : null);
                    attributes.put("instanceFamily",
                        host.hostProperties() != null ? host.hostProperties().instanceFamily() : null);
                    attributes.put("autoPlacement", host.autoPlacementAsString());
                    attributes.put("hostReservationId", host.hostReservationId());
                    attributes.put("ownerId", host.ownerId());
                    attributes.put("releaseTime",
                        host.releaseTime() != null ? host.releaseTime().toString() : null);
                    attributes.put("allocationTime",
                        host.allocationTime() != null ? host.allocationTime().toString() : null);
                    attributes.put("tags", tags);

                    String name = tags.getOrDefault("Name", hostId);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(hostId)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(host)
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
            log.error("Ec2HostCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect EC2 dedicated hosts", e);
        }
        catch (Exception e)
        {
            log.error("Ec2HostCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting EC2 dedicated hosts", e);
        }
    }

}
