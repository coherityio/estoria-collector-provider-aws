package io.coherity.estoria.collector.provider.aws.compute;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.provider.aws.ARNHelper;
import io.coherity.estoria.collector.provider.aws.AbstractAwsContextAwareCollector;
import io.coherity.estoria.collector.provider.aws.AccountScope;
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
import software.amazon.awssdk.services.ec2.model.CapacityReservation;
import software.amazon.awssdk.services.ec2.model.DescribeCapacityReservationsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeCapacityReservationsResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Tag;

/**
 * Collects EC2 Capacity Reservations via the EC2 DescribeCapacityReservations API.
 */
@Slf4j
public class CapacityReservationCollector extends AbstractAwsContextAwareCollector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "CapacityReservation";

    private Ec2Client ec2Client;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("compute", "ec2", "capacity", "reservation", "aws"))
            .build();

    public CapacityReservationCollector()
    {
        log.debug("CapacityReservationCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
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
        log.debug("CapacityReservationCollector.collect called");

        if (this.ec2Client == null)
        {
            this.ec2Client = AwsClientFactory.getInstance().getEc2Client(providerContext);
        }

        String region    = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;
        String accountId = awsSessionContext.getCurrentAccountId();

        try
        {
            DescribeCapacityReservationsRequest.Builder requestBuilder =
                DescribeCapacityReservationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("CapacityReservationCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            DescribeCapacityReservationsResponse response =
                this.ec2Client.describeCapacityReservations(requestBuilder.build());
            List<CapacityReservation> reservations = response.capacityReservations();
            String nextToken = response.nextToken();

            log.debug("CapacityReservationCollector received {} reservations, nextToken={}",
                reservations != null ? reservations.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (reservations != null)
            {
                for (CapacityReservation reservation : reservations)
                {
                    if (reservation == null) continue;

                    String id  = reservation.capacityReservationId();
                    String arn = ARNHelper.ec2CapacityReservationArn(region, accountId, id);

                    // Build tag map
                    Map<String, String> tags = new HashMap<>();
                    if (reservation.tags() != null)
                    {
                        for (Tag tag : reservation.tags())
                        {
                            tags.put(tag.key(), tag.value());
                        }
                    }

                    String name = tags.getOrDefault("Name", id);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("capacityReservationId", id);
                    attributes.put("capacityReservationArn", arn);
                    attributes.put("instanceType", reservation.instanceType());
                    attributes.put("instancePlatform", reservation.instancePlatformAsString());
                    attributes.put("availabilityZone", reservation.availabilityZone());
                    attributes.put("availabilityZoneId", reservation.availabilityZoneId());
                    attributes.put("tenancy", reservation.tenancyAsString());
                    attributes.put("totalInstanceCount", reservation.totalInstanceCount());
                    attributes.put("availableInstanceCount", reservation.availableInstanceCount());
                    attributes.put("ebsOptimized", reservation.ebsOptimized());
                    attributes.put("ephemeralStorage", reservation.ephemeralStorage());
                    attributes.put("state", reservation.stateAsString());
                    attributes.put("startDate", reservation.startDate() != null
                        ? reservation.startDate().toString() : null);
                    attributes.put("endDate", reservation.endDate() != null
                        ? reservation.endDate().toString() : null);
                    attributes.put("endDateType", reservation.endDateTypeAsString());
                    attributes.put("instanceMatchCriteria", reservation.instanceMatchCriteriaAsString());
                    attributes.put("createDate", reservation.createDate() != null
                        ? reservation.createDate().toString() : null);
                    attributes.put("reservationType", reservation.reservationTypeAsString());
                    attributes.put("ownerId", reservation.ownerId());
                    attributes.put("capacityReservationFleetId", reservation.capacityReservationFleetId());
                    attributes.put("placementGroupArn", reservation.placementGroupArn());
                    attributes.put("tags", tags);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(name)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(reservation)
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
            log.error("CapacityReservationCollector EC2 error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect capacity reservations", e);
        }
        catch (Exception e)
        {
            log.error("CapacityReservationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting capacity reservations", e);
        }
    }

}
