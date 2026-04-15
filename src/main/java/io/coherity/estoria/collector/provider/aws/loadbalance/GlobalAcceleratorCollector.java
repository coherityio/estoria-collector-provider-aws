package io.coherity.estoria.collector.provider.aws.loadbalance;

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
import software.amazon.awssdk.services.globalaccelerator.GlobalAcceleratorClient;
import software.amazon.awssdk.services.globalaccelerator.model.Accelerator;
import software.amazon.awssdk.services.globalaccelerator.model.GlobalAcceleratorException;
import software.amazon.awssdk.services.globalaccelerator.model.ListAcceleratorsRequest;
import software.amazon.awssdk.services.globalaccelerator.model.ListAcceleratorsResponse;

/**
 * Collects AWS Global Accelerator accelerators via the ListAccelerators API.
 * Global Accelerator is a global service — the client is pinned to us-west-2.
 */
@Slf4j
public class GlobalAcceleratorCollector extends AbstractAwsContextAwareCollector
{
    public static final String ENTITY_TYPE = "GlobalAccelerator";

    private GlobalAcceleratorClient globalAcceleratorClient;

    public GlobalAcceleratorCollector()
    {
        super(awsCollectorInfoBuilder(ENTITY_TYPE, Set.of(), Set.of("loadbalance", "global-accelerator", "aws")).build());
        log.debug("GlobalAcceleratorCollector created");
    }

    @Override
    public AccountScope getRequiredAccountScope()
    {
        return AccountScope.MEMBER_ACCOUNT;
    }

    @Override
    public ContainmentScope getEntityContainmentScope()
    {
        return ContainmentScope.AWS_GLOBAL;
    }

    @Override
    public EntityCategory getEntityCategory()
    {
        return EntityCategory.RESOURCE;
    }

    @Override
    public CollectorCursor collectEntities(
        ProviderContext providerContext,
        AwsSessionContext awsSessionContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("GlobalAcceleratorCollector.collect called");

        if (this.globalAcceleratorClient == null)
        {
            this.globalAcceleratorClient = AwsClientFactory.getInstance().getGlobalAcceleratorClient(providerContext);
        }

        try
        {
            ListAcceleratorsRequest.Builder requestBuilder = ListAcceleratorsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("GlobalAcceleratorCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListAcceleratorsResponse response = this.globalAcceleratorClient.listAccelerators(requestBuilder.build());
            List<Accelerator> accelerators = response.accelerators();
            String nextToken = response.nextToken();

            log.debug("GlobalAcceleratorCollector received {} accelerators, nextToken={}",
                accelerators != null ? accelerators.size() : 0, nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            if (accelerators != null)
            {
                for (Accelerator accelerator : accelerators)
                {
                    if (accelerator == null) continue;

                    String arn = accelerator.acceleratorArn();

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("acceleratorArn", arn);
                    attributes.put("name", accelerator.name());
                    attributes.put("status", accelerator.statusAsString());
                    attributes.put("enabled", accelerator.enabled());
                    attributes.put("ipAddressType", accelerator.ipAddressTypeAsString());
                    attributes.put("dnsName", accelerator.dnsName());
                    attributes.put("dualStackDnsName", accelerator.dualStackDnsName());
                    attributes.put("createdTime",
                        accelerator.createdTime() != null ? accelerator.createdTime().toString() : null);
                    attributes.put("lastModifiedTime",
                        accelerator.lastModifiedTime() != null ? accelerator.lastModifiedTime().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(arn)
                            .qualifiedResourceName(arn)
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(accelerator.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(accelerator)
                        .collectedAt(now)
                        .build();

                    entities.add(entity);
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
                    return Optional.ofNullable(finalNextToken).filter(t -> !t.isBlank());
                }
                @Override public CursorMetadata getMetadata()
                {
                    return CursorMetadata.builder().values(metadataValues).build();
                }
            };
        }
        catch (GlobalAcceleratorException e)
        {
            log.error("GlobalAcceleratorCollector error: {}",
                e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Global Accelerator accelerators", e);
        }
        catch (Exception e)
        {
            log.error("GlobalAcceleratorCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Global Accelerator accelerators", e);
        }
    }
}
