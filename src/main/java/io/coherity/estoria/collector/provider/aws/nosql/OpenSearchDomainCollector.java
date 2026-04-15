package io.coherity.estoria.collector.provider.aws.nosql;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.opensearch.OpenSearchClient;
import software.amazon.awssdk.services.opensearch.model.DomainInfo;
import software.amazon.awssdk.services.opensearch.model.DomainStatus;
import software.amazon.awssdk.services.opensearch.model.ListDomainNamesRequest;
import software.amazon.awssdk.services.opensearch.model.ListDomainNamesResponse;
import software.amazon.awssdk.services.opensearch.model.DescribeDomainsRequest;
import software.amazon.awssdk.services.opensearch.model.DescribeDomainsResponse;
import software.amazon.awssdk.services.opensearch.model.OpenSearchException;
import software.amazon.awssdk.services.opensearch.model.Tag;

/**
 * Collects OpenSearch/Elasticsearch domains via the OpenSearch ListDomainNames +
 * DescribeDomains APIs.
 */
@Slf4j
public class OpenSearchDomainCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "OpenSearchDomain";

    private OpenSearchClient openSearchClient;

    public OpenSearchDomainCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "nosql", "opensearch", "elasticsearch", "aws")).build());
        log.debug("OpenSearchDomainCollector created");
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
        log.debug("OpenSearchDomainCollector.collectEntities called");

        if (this.openSearchClient == null)
        {
            this.openSearchClient = AwsClientFactory.getInstance().getOpenSearchClient(providerContext);
        }

        try
        {
            // OpenSearch ListDomainNames does not support pagination natively — returns all domains at once
            ListDomainNamesResponse listResponse = this.openSearchClient.listDomainNames(
                ListDomainNamesRequest.builder().build());

            List<DomainInfo> domainInfos = listResponse.domainNames();

            if (domainInfos == null || domainInfos.isEmpty())
            {
                log.debug("OpenSearchDomainCollector: no domains found");
                return emptyCollectorCursor();
            }

            // Batch-describe up to 5 domains at a time (API limit)
            List<String> domainNames = domainInfos.stream()
                .map(DomainInfo::domainName)
                .filter(n -> n != null && !n.isBlank())
                .collect(Collectors.toList());

            log.debug("OpenSearchDomainCollector describing {} domains", domainNames.size());

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            int batchSize = 5;
            for (int i = 0; i < domainNames.size(); i += batchSize)
            {
                List<String> batch = domainNames.subList(i, Math.min(i + batchSize, domainNames.size()));

                DescribeDomainsResponse describeResponse = this.openSearchClient.describeDomains(
                    DescribeDomainsRequest.builder().domainNames(batch).build());

                if (describeResponse.domainStatusList() == null) continue;

                for (DomainStatus domain : describeResponse.domainStatusList())
                {
                    if (domain == null) continue;

                    String domainName = domain.domainName();
                    String arn        = domain.arn();
                    String domainId   = domain.domainId();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("domainName", domainName);
                    attributes.put("domainArn", arn);
                    attributes.put("domainId", domainId);
                    attributes.put("engineVersion", domain.engineVersion());
                    attributes.put("created", domain.created());
                    attributes.put("deleted", domain.deleted());
                    attributes.put("processing", domain.processing());
                    attributes.put("upgradeProcessing", domain.upgradeProcessing());
                    attributes.put("endpoint", domain.endpoint());
                    if (domain.clusterConfig() != null)
                    {
                        attributes.put("instanceType", domain.clusterConfig().instanceTypeAsString());
                        attributes.put("instanceCount", domain.clusterConfig().instanceCount());
                        attributes.put("dedicatedMasterEnabled", domain.clusterConfig().dedicatedMasterEnabled());
                        attributes.put("zoneAwarenessEnabled", domain.clusterConfig().zoneAwarenessEnabled());
                    }
                    if (domain.encryptionAtRestOptions() != null)
                    {
                        attributes.put("encryptionAtRestEnabled", domain.encryptionAtRestOptions().enabled());
                    }
                    if (domain.nodeToNodeEncryptionOptions() != null)
                    {
                        attributes.put("nodeToNodeEncryptionEnabled", domain.nodeToNodeEncryptionOptions().enabled());
                    }
                    attributes.put("vpcEndpoint",
                        domain.endpoints() != null ? domain.endpoints().get("vpc") : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : domainId)
                            .qualifiedResourceName(arn != null ? arn : domainId)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(domainName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(domain)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
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
        catch (OpenSearchException e)
        {
            log.error("OpenSearchDomainCollector OpenSearch error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect OpenSearch domains", e);
        }
        catch (Exception e)
        {
            log.error("OpenSearchDomainCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting OpenSearch domains", e);
        }
    }

    private static CollectorCursor emptyCollectorCursor()
    {
        Map<String, Object> metadataValues = new HashMap<>();
        metadataValues.put("count", 0);
        return new CollectorCursor()
        {
            @Override public List<CloudEntity> getEntities() { return List.of(); }
            @Override public Optional<String> getNextCursorToken() { return Optional.empty(); }
            @Override public CursorMetadata getMetadata()
            {
                return CursorMetadata.builder().values(metadataValues).build();
            }
        };
    }
}
