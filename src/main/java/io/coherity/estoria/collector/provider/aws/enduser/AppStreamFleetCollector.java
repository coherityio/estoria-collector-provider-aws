package io.coherity.estoria.collector.provider.aws.enduser;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.appstream.AppStreamClient;
import software.amazon.awssdk.services.appstream.model.DescribeFleetsRequest;
import software.amazon.awssdk.services.appstream.model.DescribeFleetsResponse;
import software.amazon.awssdk.services.appstream.model.Fleet;

@Slf4j
public class AppStreamFleetCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppStreamFleet";


    public AppStreamFleetCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("enduser", "appstream", "fleet", "aws")).build());
        log.debug("AppStreamFleetCollector created");
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
        AppStreamClient appStreamClient = AwsClientFactory.getInstance().getAppStreamClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeFleetsRequest.Builder requestBuilder = DescribeFleetsRequest.builder();

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeFleetsResponse response = appStreamClient.describeFleets(requestBuilder.build());
            List<Fleet> fleets = response.fleets();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (fleets != null)
            {
                for (Fleet fleet : fleets)
                {
                    if (fleet == null)
                    {
                        continue;
                    }

                    String fleetName = fleet.name();
                    String fleetArn = ARNHelper.appStreamFleetArn(region, accountId, fleetName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("fleetName", fleetName);
                    attributes.put("fleetArn", fleetArn);
                    attributes.put("displayName", fleet.displayName());
                    attributes.put("description", fleet.description());
                    attributes.put("state", fleet.stateAsString());
                    attributes.put("instanceType", fleet.instanceType());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(fleetName)
                            .qualifiedResourceName(fleetArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(fleet.displayName() != null ? fleet.displayName() : fleetName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(fleet)
                        .collectedAt(now)
                        .build());
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
                    return Optional.ofNullable(finalNextToken).filter(token -> !token.isBlank());
                }

                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (Exception e)
        {
            log.error("AppStreamFleetCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting AppStream fleets", e);
        }
    }
}