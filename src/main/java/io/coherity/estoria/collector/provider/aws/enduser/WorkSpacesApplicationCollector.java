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
import software.amazon.awssdk.services.workspaces.WorkSpacesClient;
import software.amazon.awssdk.services.workspaces.model.DescribeApplicationsRequest;
import software.amazon.awssdk.services.workspaces.model.DescribeApplicationsResponse;
import software.amazon.awssdk.services.workspaces.model.WorkSpaceApplication;

@Slf4j
public class WorkSpacesApplicationCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "WorkSpacesApplication";


    public WorkSpacesApplicationCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("enduser", "workspaces", "application", "aws")).build());
        log.debug("WorkSpacesApplicationCollector created");
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
        WorkSpacesClient workspacesClient = AwsClientFactory.getInstance().getWorkspacesClient(providerContext);

        try
        {
            String accountId = awsSessionContext.getCurrentAccountId();
            String region = awsSessionContext.getRegion() != null ? awsSessionContext.getRegion().id() : null;

            DescribeApplicationsRequest.Builder requestBuilder = DescribeApplicationsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(requestBuilder::nextToken);

            DescribeApplicationsResponse response = workspacesClient.describeApplications(requestBuilder.build());
            List<WorkSpaceApplication> applications = response.applications();
            String nextToken = response.nextToken();

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (applications != null)
            {
                for (WorkSpaceApplication application : applications)
                {
                    if (application == null)
                    {
                        continue;
                    }

                    String applicationId = application.applicationId();
                    String applicationArn = ARNHelper.workSpacesApplicationArn(region, accountId, applicationId);

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("applicationId", applicationId);
                    attributes.put("applicationArn", applicationArn);
                    attributes.put("name", application.name());
                    attributes.put("description", application.description());
                    attributes.put("licenseType", application.licenseTypeAsString());
                    attributes.put("owner", application.owner());
                    attributes.put("state", application.stateAsString());
                    attributes.put("accountId", accountId);
                    attributes.put("region", region);

                    entities.add(CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(applicationId)
                            .qualifiedResourceName(applicationArn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(application.name() != null ? application.name() : applicationId)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(application)
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
            log.error("WorkSpacesApplicationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WorkSpaces applications", e);
        }
    }
}