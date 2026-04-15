package io.coherity.estoria.collector.provider.aws.rds;

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
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBProxy;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbProxiesResponse;
import software.amazon.awssdk.services.rds.model.RdsException;

/**
 * Collects RDS proxies via the RDS DescribeDBProxies API.
 */
@Slf4j
public class RdsProxyCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "RdsProxy";

    private RdsClient rdsClient;

    public RdsProxyCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("database", "rds", "proxy", "aws")).build());
        log.debug("RdsProxyCollector created");
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
        log.debug("RdsProxyCollector.collectEntities called");

        if (this.rdsClient == null)
        {
            this.rdsClient = AwsClientFactory.getInstance().getRdsClient(providerContext);
        }

        try
        {
            DescribeDbProxiesRequest.Builder requestBuilder = DescribeDbProxiesRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxRecords(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("RdsProxyCollector resuming from marker: {}", token);
                requestBuilder.marker(token);
            });

            DescribeDbProxiesResponse response = this.rdsClient.describeDBProxies(requestBuilder.build());
            List<DBProxy> proxies = response.dbProxies();
            String nextMarker = response.marker();

            log.debug("RdsProxyCollector received {} proxies, nextMarker={}",
                proxies != null ? proxies.size() : 0, nextMarker);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (proxies != null)
            {
                for (DBProxy proxy : proxies)
                {
                    if (proxy == null) continue;

                    String proxyName = proxy.dbProxyName();
                    String arn = proxy.dbProxyArn();

                    List<String> vpcSecurityGroupIds = proxy.vpcSecurityGroupIds() != null
                        ? proxy.vpcSecurityGroupIds() : List.of();
                    List<String> vpcSubnetIds = proxy.vpcSubnetIds() != null
                        ? proxy.vpcSubnetIds() : List.of();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("dbProxyName", proxyName);
                    attributes.put("dbProxyArn", arn);
                    attributes.put("status", proxy.statusAsString());
                    attributes.put("engineFamily", proxy.engineFamily());
                    attributes.put("endpoint", proxy.endpoint());
                    attributes.put("vpcId", proxy.vpcId());
                    attributes.put("vpcSecurityGroupIds", vpcSecurityGroupIds);
                    attributes.put("vpcSubnetIds", vpcSubnetIds);
                    attributes.put("roleArn", proxy.roleArn());
                    attributes.put("requireTLS", proxy.requireTLS());
                    attributes.put("idleClientTimeout", proxy.idleClientTimeout());
                    attributes.put("debugLogging", proxy.debugLogging());
                    attributes.put("createdDate",
                        proxy.createdDate() != null ? proxy.createdDate().toString() : null);
                    attributes.put("updatedDate",
                        proxy.updatedDate() != null ? proxy.updatedDate().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn != null ? arn : proxyName)
                            .qualifiedResourceName(arn != null ? arn : proxyName)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(proxyName)
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(proxy)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
                }
            }

            String finalNextMarker = nextMarker;
            Map<String, Object> metadataValues = new HashMap<>();
            metadataValues.put("count", entities.size());

            return new CollectorCursor()
            {
                @Override public List<CloudEntity> getEntities() { return entities; }
                @Override public Optional<String> getNextCursorToken()
                {
                    return Optional.ofNullable(finalNextMarker).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (RdsException e)
        {
            log.error("RdsProxyCollector RDS error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect RDS proxies", e);
        }
        catch (Exception e)
        {
            log.error("RdsProxyCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting RDS proxies", e);
        }
    }
}
