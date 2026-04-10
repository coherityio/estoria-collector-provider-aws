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
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.ListSecretsRequest;
import software.amazon.awssdk.services.secretsmanager.model.ListSecretsResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretListEntry;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

/**
 * Collects all secrets in AWS Secrets Manager.
 */
@Slf4j
public class SecretsManagerSecretCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SecretsManagerSecret";
    private static final int PAGE_SIZE = 100;

    private SecretsManagerClient secretsManagerClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "secretsmanager", "aws"))
            .build();

    public SecretsManagerSecretCollector()
    {
        log.debug("SecretsManagerSecretCollector created");
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
        log.debug("SecretsManagerSecretCollector.collect called");

        if (this.secretsManagerClient == null)
        {
            this.secretsManagerClient = AwsClientFactory.getInstance().getSecretsManagerClient(providerContext);
        }

        try
        {
            List<CloudEntity> entities = new ArrayList<>();
            String nextToken = null;

            do
            {
                ListSecretsResponse response = this.secretsManagerClient.listSecrets(
                    ListSecretsRequest.builder()
                        .maxResults(PAGE_SIZE)
                        .nextToken(nextToken)
                        .build());

                for (SecretListEntry secret : response.secretList())
                {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put("name", secret.name());
                    attributes.put("arn", secret.arn());
                    attributes.put("description", secret.description());
                    attributes.put("kmsKeyId", secret.kmsKeyId());
                    attributes.put("rotationEnabled", secret.rotationEnabled());
                    attributes.put("rotationLambdaArn", secret.rotationLambdaARN());
                    attributes.put("lastChangedDate", secret.lastChangedDate() != null ? secret.lastChangedDate().toString() : null);
                    attributes.put("lastAccessedDate", secret.lastAccessedDate() != null ? secret.lastAccessedDate().toString() : null);
                    attributes.put("lastRotatedDate", secret.lastRotatedDate() != null ? secret.lastRotatedDate().toString() : null);
                    attributes.put("createdDate", secret.createdDate() != null ? secret.createdDate().toString() : null);
                    attributes.put("deletedDate", secret.deletedDate() != null ? secret.deletedDate().toString() : null);

                    CloudEntity entity = CloudEntity.builder()
                        .entityIdentifier(EntityIdentifier.builder()
                            .id(secret.arn())
                            .qualifiedResourceName(secret.arn())
                            .build())
                        .entityType(ENTITY_TYPE)
                        .name(secret.name())
                        .collectorContext(collectorContext)
                        .attributes(attributes)
                        .rawPayload(secret)
                        .collectedAt(Instant.now())
                        .build();
                    entities.add(entity);
                }

                nextToken = response.nextToken();
            }
            while (nextToken != null);

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
        catch (SecretsManagerException e)
        {
            log.error("SecretsManagerSecretCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Secrets Manager secrets", e);
        }
        catch (Exception e)
        {
            log.error("SecretsManagerSecretCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Secrets Manager secrets", e);
        }
    }
}
