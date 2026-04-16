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
import software.amazon.awssdk.services.appstream.model.DescribeStacksRequest;
import software.amazon.awssdk.services.appstream.model.DescribeStacksResponse;
import software.amazon.awssdk.services.appstream.model.Stack;

@Slf4j
public class AppStreamStackCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "AppStreamStack";


    public AppStreamStackCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("enduser", "appstream", "stack", "aws")).build());
        log.debug("AppStreamStackCollector created");
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

            DescribeStacksRequest.Builder requestBuilder = DescribeStacksRequest.builder();

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeStacksResponse response = appStreamClient.describeStacks(requestBuilder.build());
            List<Stack> stacks = response.stacks();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (stacks != null)
            {
                for (Stack stack : stacks)
                {
                    if (stack == null)
                    {
                        continue;
                    }

                    String stackName = stack.name();
                    String stackArn = ARNHelper.appStreamStackArn(region, accountId, stackName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("stackName", stackName);
                    attributes.put("stackArn", stackArn);
                    attributes.put("displayName", stack.displayName());
                    attributes.put("description", stack.description());
                    attributes.put("feedbackUrl", stack.feedbackURL());
                    attributes.put("redirectUrl", stack.redirectURL());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(stackName)
                            .qualifiedResourceName(stackArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(stack.displayName() != null ? stack.displayName() : stackName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(stack)
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
            log.error("AppStreamStackCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting AppStream stacks", e);
        }
    }
}