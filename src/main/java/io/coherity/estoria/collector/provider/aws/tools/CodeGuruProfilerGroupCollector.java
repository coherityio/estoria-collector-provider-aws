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
import software.amazon.awssdk.services.codeguruprofiler.CodeGuruProfilerClient;
import software.amazon.awssdk.services.codeguruprofiler.model.CodeGuruProfilerException;
import software.amazon.awssdk.services.codeguruprofiler.model.ListProfilingGroupsRequest;
import software.amazon.awssdk.services.codeguruprofiler.model.ListProfilingGroupsResponse;

@Slf4j
public class CodeGuruProfilerGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeGuruProfilerGroup";


    public CodeGuruProfilerGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codeguru-profiler", "profiling", "aws")).build());
        log.debug("CodeGuruProfilerGroupCollector created");
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
        CodeGuruProfilerClient codeGuruProfilerClient = AwsClientFactory.getInstance().getCodeGuruProfilerClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListProfilingGroupsRequest.Builder requestBuilder = ListProfilingGroupsRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListProfilingGroupsResponse response = codeGuruProfilerClient.listProfilingGroups(requestBuilder.build());
            List<String> profilingGroupNames = response.profilingGroupNames();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (profilingGroupNames != null)
            {
                for (String profilingGroupName : profilingGroupNames)
                {
                    if (profilingGroupName == null || profilingGroupName.isBlank()) continue;

                    String profilingGroupArn = ARNHelper.codeGuruProfilerGroupArn(region, accountId, profilingGroupName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("profilingGroupName", profilingGroupName);
                    attributes.put("profilingGroupArn", profilingGroupArn);
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(profilingGroupName)
                            .qualifiedResourceName(profilingGroupArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(profilingGroupName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(profilingGroupName)
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
        catch (CodeGuruProfilerException e)
        {
            throw new CollectorException("Failed to collect CodeGuru Profiler groups", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeGuru Profiler groups", e);
        }
    }
}