package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
import software.amazon.awssdk.services.config.ConfigClient;
import software.amazon.awssdk.services.config.model.ConfigException;
import software.amazon.awssdk.services.config.model.ConfigurationRecorder;
import software.amazon.awssdk.services.config.model.ConfigurationRecorderStatus;
import software.amazon.awssdk.services.config.model.DeliveryChannel;
import software.amazon.awssdk.services.config.model.DescribeConfigurationRecorderStatusRequest;
import software.amazon.awssdk.services.config.model.DescribeConfigurationRecorderStatusResponse;
import software.amazon.awssdk.services.config.model.DescribeConfigurationRecordersRequest;
import software.amazon.awssdk.services.config.model.DescribeConfigurationRecordersResponse;
import software.amazon.awssdk.services.config.model.DescribeDeliveryChannelsRequest;
import software.amazon.awssdk.services.config.model.DescribeDeliveryChannelsResponse;

/**
 * Collects AWS Config configuration recorders and delivery channels.
 * Emits one entity per recorder and one per delivery channel.
 */
@Slf4j
public class ConfigRecorderCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "ConfigRecorder";


    public ConfigRecorderCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "config", "aws")).build());
        log.debug("ConfigRecorderCollector created");
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
        log.debug("ConfigRecorderCollector.collect called");

        ConfigClient configClient = AwsClientFactory.getInstance().getConfigClient(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();

            // Describe all configuration recorders
            DescribeConfigurationRecordersResponse recordersResponse = configClient.describeConfigurationRecorders(
                DescribeConfigurationRecordersRequest.builder().build());

            // Get status for all recorders
            DescribeConfigurationRecorderStatusResponse statusResponse = configClient.describeConfigurationRecorderStatus(
                DescribeConfigurationRecorderStatusRequest.builder().build());

            Map<String, ConfigurationRecorderStatus> statusMap = new HashMap<>();
            for (ConfigurationRecorderStatus status : statusResponse.configurationRecordersStatus())
            {
                statusMap.put(status.name(), status);
            }

            for (ConfigurationRecorder recorder : recordersResponse.configurationRecorders())
            {
                ConfigurationRecorderStatus status = statusMap.get(recorder.name());

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("name", recorder.name());
                attributes.put("roleArn", recorder.roleARN());
                attributes.put("entityType", "ConfigurationRecorder");

                if (recorder.recordingGroup() != null)
                {
                    attributes.put("allSupported", recorder.recordingGroup().allSupported());
                    attributes.put("includeGlobalResourceTypes", recorder.recordingGroup().includeGlobalResourceTypes());
                }

                if (status != null)
                {
                    attributes.put("recording", status.recording());
                    attributes.put("lastStatus", status.lastStatusAsString());
                    attributes.put("lastStartTime", status.lastStartTime() != null ? status.lastStartTime().toString() : null);
                    attributes.put("lastStopTime", status.lastStopTime() != null ? status.lastStopTime().toString() : null);
                }

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id("config-recorder/" + recorder.name())
                        .qualifiedResourceName("config-recorder/" + recorder.name())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(recorder.name())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(recorder)
                    .collectedAt(Instant.now())
                    .build();
                entities.add(entity);
            }

            // Describe delivery channels
            DescribeDeliveryChannelsResponse channelsResponse = configClient.describeDeliveryChannels(
                DescribeDeliveryChannelsRequest.builder().build());

            for (DeliveryChannel channel : channelsResponse.deliveryChannels())
            {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("name", channel.name());
                attributes.put("s3BucketName", channel.s3BucketName());
                attributes.put("s3KeyPrefix", channel.s3KeyPrefix());
                attributes.put("s3KmsKeyArn", channel.s3KmsKeyArn());
                attributes.put("snsTopicArn", channel.snsTopicARN());
                attributes.put("entityType", "DeliveryChannel");

                if (channel.configSnapshotDeliveryProperties() != null)
                {
                    attributes.put("deliveryFrequency", channel.configSnapshotDeliveryProperties().deliveryFrequencyAsString());
                }

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id("config-delivery-channel/" + channel.name())
                        .qualifiedResourceName("config-delivery-channel/" + channel.name())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(channel.name())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(channel)
                    .collectedAt(Instant.now())
                    .build();
                entities.add(entity);
            }

            final int count = entities.size();
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", count);

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
        catch (ConfigException e)
        {
            log.error("ConfigRecorderCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Config recorders", e);
        }
        catch (Exception e)
        {
            log.error("ConfigRecorderCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Config recorders", e);
        }
    }
}
