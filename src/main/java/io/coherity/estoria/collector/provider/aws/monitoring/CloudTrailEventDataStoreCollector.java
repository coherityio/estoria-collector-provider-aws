package io.coherity.estoria.collector.provider.aws.monitoring;

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
import software.amazon.awssdk.services.cloudtrail.CloudTrailClient;
import software.amazon.awssdk.services.cloudtrail.model.EventDataStore;
import software.amazon.awssdk.services.cloudtrail.model.ListEventDataStoresRequest;
import software.amazon.awssdk.services.cloudtrail.model.ListEventDataStoresResponse;

@Slf4j
public class CloudTrailEventDataStoreCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudTrailEventDataStore";


    public CloudTrailEventDataStoreCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "cloudtrail", "lake", "event-data-store", "aws")).build());
    }

    @Override public AccountScope getRequiredAccountScope() { return AccountScope.MEMBER_ACCOUNT; }
    @Override public ContainmentScope getEntityContainmentScope() { return ContainmentScope.ACCOUNT_REGIONAL; }
    @Override public EntityCategory getEntityCategory() { return EntityCategory.RESOURCE; }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        CloudTrailClient cloudTrailClient = AwsClientFactory.getInstance().getCloudTrailClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListEventDataStoresRequest.Builder requestBuilder = ListEventDataStoresRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListEventDataStoresResponse response = cloudTrailClient.listEventDataStores(requestBuilder.build());
            List<EventDataStore> eventDataStores = response.eventDataStores();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (eventDataStores != null)
            {
                for (EventDataStore eventDataStore : eventDataStores)
                {
                    if (eventDataStore == null)
                    {
                        continue;
                    }

                    String eventDataStoreArn = eventDataStore.eventDataStoreArn();
                    String eventDataStoreName = eventDataStore.name();
                    String identifier = eventDataStoreArn != null ? eventDataStoreArn : eventDataStoreName;
                    String qualifiedName = eventDataStoreArn != null
                        ? eventDataStoreArn
                        : ARNHelper.cloudTrailEventDataStoreArn(region, accountId, eventDataStoreName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("eventDataStoreArn", eventDataStoreArn);
                    attributes.put("name", eventDataStoreName);
                    attributes.put("status", eventDataStore.statusAsString());
                    attributes.put("advancedEventSelectors", eventDataStore.advancedEventSelectors());
                    attributes.put("multiRegionEnabled", eventDataStore.multiRegionEnabled());
                    attributes.put("organizationEnabled", eventDataStore.organizationEnabled());
                    attributes.put("retentionPeriod", eventDataStore.retentionPeriod());
                    attributes.put("terminationProtectionEnabled", eventDataStore.terminationProtectionEnabled());
                    attributes.put("createdTimestamp", eventDataStore.createdTimestamp());
                    attributes.put("updatedTimestamp", eventDataStore.updatedTimestamp());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(identifier)
                            .qualifiedResourceName(qualifiedName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(eventDataStoreName != null ? eventDataStoreName : identifier)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(eventDataStore)
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
            log.error("CloudTrailEventDataStoreCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudTrail event data stores", e);
        }
    }
}