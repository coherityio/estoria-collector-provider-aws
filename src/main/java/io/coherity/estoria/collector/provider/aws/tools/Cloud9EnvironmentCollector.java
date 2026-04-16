package io.coherity.estoria.collector.provider.aws.tools;

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
import software.amazon.awssdk.services.cloud9.Cloud9Client;
import software.amazon.awssdk.services.cloud9.model.Cloud9Exception;
import software.amazon.awssdk.services.cloud9.model.DescribeEnvironmentsRequest;
import software.amazon.awssdk.services.cloud9.model.Environment;
import software.amazon.awssdk.services.cloud9.model.ListEnvironmentsRequest;
import software.amazon.awssdk.services.cloud9.model.ListEnvironmentsResponse;

@Slf4j
public class Cloud9EnvironmentCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "Cloud9Environment";


    public Cloud9EnvironmentCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "cloud9", "ide", "aws")).build());
        log.debug("Cloud9EnvironmentCollector created");
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
        Cloud9Client cloud9Client = AwsClientFactory.getInstance().getCloud9Client(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListEnvironmentsRequest.Builder requestBuilder = ListEnvironmentsRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListEnvironmentsResponse response = cloud9Client.listEnvironments(requestBuilder.build());
            List<String> environmentIds = response.environmentIds();
            String nextToken = response.nextToken();

            List<Environment> environments = environmentIds == null || environmentIds.isEmpty()
                ? List.of()
                : cloud9Client.describeEnvironments(DescribeEnvironmentsRequest.builder().environmentIds(environmentIds).build()).environments();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Environment environment : environments)
            {
                if (environment == null) continue;

                String environmentId = environment.id();
                String environmentArn = ARNHelper.cloud9EnvironmentArn(region, accountId, environmentId);

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("environmentId", environmentId);
                attributes.put("environmentArn", environmentArn);
                attributes.put("name", environment.name());
                attributes.put("description", environment.description());
                attributes.put("type", environment.typeAsString());
                attributes.put("accountId", accountId);
                attributes.put("region", region);

                entities.add(CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(environmentId)
                        .qualifiedResourceName(environmentArn)
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(environment.name())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(environment)
                    .collectedAt(now)
                    .build());
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
        catch (Cloud9Exception e)
        {
            throw new CollectorException("Failed to collect Cloud9 environments", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting Cloud9 environments", e);
        }
    }
}