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
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.AlarmType;
import software.amazon.awssdk.services.cloudwatch.model.DescribeAlarmsRequest;
import software.amazon.awssdk.services.cloudwatch.model.DescribeAlarmsResponse;
import software.amazon.awssdk.services.cloudwatch.model.MetricAlarm;

@Slf4j
public class CloudWatchMetricAlarmCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CloudWatchMetricAlarm";


    public CloudWatchMetricAlarmCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("monitoring", "cloudwatch", "alarm", "metric", "aws")).build());
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
        CloudWatchClient cloudWatchClient = AwsClientFactory.getInstance().getCloudWatchClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeAlarmsRequest.Builder requestBuilder = DescribeAlarmsRequest.builder().alarmTypes(AlarmType.METRIC_ALARM);

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeAlarmsResponse response = cloudWatchClient.describeAlarms(requestBuilder.build());
            List<MetricAlarm> alarms = response.metricAlarms();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (alarms != null)
            {
                for (MetricAlarm alarm : alarms)
                {
                    if (alarm == null)
                    {
                        continue;
                    }

                    String alarmName = alarm.alarmName();
                    String qualifiedName = alarm.alarmArn() != null
                        ? alarm.alarmArn()
                        : ARNHelper.cloudWatchAlarmArn(region, accountId, alarmName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("alarmName", alarmName);
                    attributes.put("alarmArn", alarm.alarmArn());
                    attributes.put("alarmDescription", alarm.alarmDescription());
                    attributes.put("actionsEnabled", alarm.actionsEnabled());
                    attributes.put("okActions", alarm.okActions());
                    attributes.put("alarmActions", alarm.alarmActions());
                    attributes.put("insufficientDataActions", alarm.insufficientDataActions());
                    attributes.put("stateValue", alarm.stateValueAsString());
                    attributes.put("stateReason", alarm.stateReason());
                    attributes.put("stateReasonData", alarm.stateReasonData());
                    attributes.put("stateUpdatedTimestamp", alarm.stateUpdatedTimestamp());
                    attributes.put("metricName", alarm.metricName());
                    attributes.put("namespace", alarm.namespace());
                    attributes.put("statistic", alarm.statisticAsString());
                    attributes.put("extendedStatistic", alarm.extendedStatistic());
                    attributes.put("dimensions", alarm.dimensions());
                    attributes.put("period", alarm.period());
                    attributes.put("unit", alarm.unitAsString());
                    attributes.put("evaluationPeriods", alarm.evaluationPeriods());
                    attributes.put("datapointsToAlarm", alarm.datapointsToAlarm());
                    attributes.put("threshold", alarm.threshold());
                    attributes.put("comparisonOperator", alarm.comparisonOperatorAsString());
                    attributes.put("treatMissingData", alarm.treatMissingData());
                    attributes.put("evaluateLowSampleCountPercentile", alarm.evaluateLowSampleCountPercentile());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(alarmName)
                            .qualifiedResourceName(qualifiedName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(alarmName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(alarm)
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
            log.error("CloudWatchMetricAlarmCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting CloudWatch metric alarms", e);
        }
    }
}