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
import software.amazon.awssdk.services.codedeploy.model.ListDeploymentGroupsRequest;
import software.amazon.awssdk.services.codedeploy.model.ListDeploymentGroupsResponse;

@Slf4j
public class CodeDeployDeploymentGroupCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "CodeDeployDeploymentGroup";


    public CodeDeployDeploymentGroupCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(CodeDeployApplicationCollector.ENTITY_TYPE), Set.of("tools", "codedeploy", "deployment-group", "aws")).build());
        log.debug("CodeDeployDeploymentGroupCollector created");
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

            ListApplicationsResponse appResponse = codeDeployClient.listApplications(ListApplicationsRequest.builder().build());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (appResponse.applications() != null)
            {
                for (String applicationName : appResponse.applications())
                {
                    if (applicationName == null || applicationName.isBlank()) continue;

                    ListDeploymentGroupsRequest.Builder requestBuilder = ListDeploymentGroupsRequest.builder()
                        .applicationName(applicationName);
                    collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

                    ListDeploymentGroupsResponse groupResponse = codeDeployClient.listDeploymentGroups(requestBuilder.build());
                    if (groupResponse.deploymentGroups() == null)
                    {
                        continue;
                    }

                    for (String deploymentGroupName : groupResponse.deploymentGroups())
                    {
                        if (deploymentGroupName == null || deploymentGroupName.isBlank()) continue;

                        String deploymentGroupArn = ARNHelper.codeDeployDeploymentGroupArn(region, accountId, applicationName, deploymentGroupName);
                        String entityId = applicationName + ":" + deploymentGroupName;

                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("applicationName", applicationName);
                        attributes.put("deploymentGroupName", deploymentGroupName);
                        attributes.put("deploymentGroupArn", deploymentGroupArn);
                        attributes.put("accountId", accountId);
                        attributes.put("region", region);

                        entities.add(CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(entityId)
                                .qualifiedResourceName(deploymentGroupArn)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(deploymentGroupName)
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(Map.of("applicationName", applicationName, "deploymentGroupName", deploymentGroupName))
                            .collectedAt(now)
                            .build());
                    }
                }
            }

            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

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
        catch (CodeDeployException e)
        {
            throw new CollectorException("Failed to collect CodeDeploy deployment groups", e);
        }
        catch (Exception e)
        {
            throw new CollectorException("Unexpected error collecting CodeDeploy deployment groups", e);
        }
    }
}