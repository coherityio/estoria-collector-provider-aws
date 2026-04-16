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
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.wafv2.Wafv2Client;
import software.amazon.awssdk.services.wafv2.model.ListWebAcLsRequest;
import software.amazon.awssdk.services.wafv2.model.ListWebAcLsResponse;
import software.amazon.awssdk.services.wafv2.model.Scope;
import software.amazon.awssdk.services.wafv2.model.WebACLSummary;

/**
 * Collects AWS WAFv2 Web ACLs (REGIONAL scope).
 */
@Slf4j
public class WafWebAclCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "WafWebAcl";


    public WafWebAclCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("security", "waf", "aws")).build());
        log.debug("WafWebAclCollector created");
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
        log.debug("WafWebAclCollector.collect called");

        Wafv2Client wafv2Client = AwsClientFactory.getInstance().getWafv2Client(providerContext);

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextMarker = null;

            do
            {
            	ListWebAcLsRequest.Builder wafRequestBuilder = ListWebAcLsRequest.builder()
                    .scope(Scope.REGIONAL)
                    .limit(100);
                if (nextMarker != null)
                {
                    wafRequestBuilder.nextMarker(nextMarker);
                }
                ListWebAcLsResponse response = wafv2Client.listWebACLs(wafRequestBuilder.build());

                for (WebACLSummary acl : response.webACLs())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("id", acl.id());
                    attributes.put("name", acl.name());
                    attributes.put("arn", acl.arn());
                    attributes.put("description", acl.description());
                    attributes.put("lockToken", acl.lockToken());

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(acl.arn())
                            .qualifiedResourceName(acl.arn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(acl.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(acl)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextMarker = response.nextMarker();
            }
            while (nextMarker != null);

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
        catch (AwsServiceException e)
        {
            log.error("WafWebAclCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect WAFv2 Web ACLs", e);
        }
        catch (Exception e)
        {
            log.error("WafWebAclCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting WAFv2 Web ACLs", e);
        }
    }
}
