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
import software.amazon.awssdk.services.codedeploy.CodeDeployClient;
import software.amazon.awssdk.services.codedeploy.model.CodeDeployException;
import software.amazon.awssdk.services.codedeploy.model.ListApplicationsRequest;
import software.amazon.awssdk.services.codedeploy.model.ListApplicationsResponse;

@Slf4j
public class CodeDeployApplicationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeDeployApplication";


    public CodeDeployApplicationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("tools", "codedeploy", "deployment", "aws")).build());
        log.debug("CodeDeployApplicationCollector created");
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
        CodeDeployClient codeDeployClient = AwsClientFactory.getInstance().getCodeDeployClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            ListApplicationsRequest.Builder requestBuilder = ListApplicationsRequest.builder();
            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            ListApplicationsResponse response = codeDeployClient.listApplications(requestBuilder.build());
            List<String> applications = response.applications();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (applications != null)
            {
                for (String applicationName : applications)
                {
                    if (applicationName == null || applicationName.isBlank()) continue;

                    String applicationArn = ARNHelper.codeDeployApplicationArn(region, accountId, applicationName);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("applicationName", applicationName);
                    attributes.put("applicationArn", applicationArn);
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(applicationName)
                            .qualifiedResourceName(applicationArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(applicationName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(applicationName)
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
        catch (CodeDeployException e)
        {
            throw new CollectorException("Failed to collect CodeDeploy applications", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeDeploy applications", e);
        }
    }
}