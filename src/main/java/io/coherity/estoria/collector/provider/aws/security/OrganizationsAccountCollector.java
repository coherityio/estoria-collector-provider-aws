package io.coherity.estoria.collector.provider.aws.security;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.AwsClientFactory;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.EntityIdentifier;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.Account;
import software.amazon.awssdk.services.organizations.model.ListAccountsRequest;
import software.amazon.awssdk.services.organizations.model.ListAccountsResponse;
import software.amazon.awssdk.services.organizations.model.OrganizationsException;

/**
 * Collects AWS Organizations accounts via the Organizations ListAccounts API.
 */
@Slf4j
public class OrganizationsAccountCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "OrganizationsAccount";

    private OrganizationsClient organizationsClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "organizations", "account", "aws"))
            .build();

    public OrganizationsAccountCollector()
    {
        log.debug("OrganizationsAccountCollector created");
    }

    @Override
    public CollectorInfo getCollectorInfo()
    {
        return this.collectorInfo;
    }

    @Override
    public CollectorCursor collect(
        ProviderContext providerContext,
        CollectorContext collectorContext,
        CollectorRequestParams collectorRequestParams) throws CollectorException
    {
        log.debug("OrganizationsAccountCollector.collect called");

        if (this.organizationsClient == null)
        {
            this.organizationsClient = AwsClientFactory.getInstance().getOrganizationsClient(providerContext);
        }

        try
        {
            ListAccountsRequest.Builder requestBuilder = ListAccountsRequest.builder();

            int pageSize = collectorRequestParams.getPageSize();
            if (pageSize > 0)
            {
                requestBuilder.maxResults(pageSize);
            }

            collectorRequestParams.getCursorToken().ifPresent(token -> {
                log.debug("OrganizationsAccountCollector resuming from nextToken: {}", token);
                requestBuilder.nextToken(token);
            });

            ListAccountsResponse response = this.organizationsClient.listAccounts(requestBuilder.build());
            List<Account> accounts = response.accounts();
            String nextToken = response.nextToken();

            log.debug("OrganizationsAccountCollector received {} accounts, nextToken={}", accounts.size(), nextToken);

            List<CloudEntity> entities = new ArrayList<>();
            Instant now = Instant.now();

            for (Account account : accounts)
            {
                if (account == null) continue;

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("accountId", account.id());
                attributes.put("accountName", account.name());
                attributes.put("arn", account.arn());
                attributes.put("email", account.email());
                attributes.put("status", account.statusAsString());
                attributes.put("joinedMethod", account.joinedMethodAsString());
                attributes.put("joinedTimestamp", account.joinedTimestamp() != null ? account.joinedTimestamp().toString() : null);

                CloudEntity entity = CloudEntity.builder()
                    .entityIdentifier(EntityIdentifier.builder()
                        .id(account.arn())
                        .qualifiedResourceName(account.arn())
                        .build())
                    .entityType(ENTITY_TYPE)
                    .name(account.name())
                    .collectorContext(collectorContext)
                    .attributes(attributes)
                    .rawPayload(account)
                    .collectedAt(now)
                    .build();

                entities.add(entity);
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
        catch (OrganizationsException e)
        {
            log.error("OrganizationsAccountCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Organizations accounts", e);
        }
        catch (Exception e)
        {
            log.error("OrganizationsAccountCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Organizations accounts", e);
        }
    }
}
