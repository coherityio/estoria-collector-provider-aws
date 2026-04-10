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
 * Collects AWS Secrets Manager secrets that have rotation configured.
 * Emits one entity per rotating secret, containing rotation policy details.
 */
@Slf4j
public class SecretsManagerRotationCollector implements Collector
{
    private static final String PROVIDER_ID = "aws";
    public  static final String ENTITY_TYPE = "SecretsManagerRotation";
    private static final int PAGE_SIZE = 100;

    private SecretsManagerClient secretsManagerClient;

    private final CollectorInfo collectorInfo =
        CollectorInfo.builder()
            .providerId(PROVIDER_ID)
            .entityType(ENTITY_TYPE)
            .requiredEntityTypes(Set.of())
            .tags(Set.of("security", "secretsmanager", "aws"))
            .build();

    public SecretsManagerRotationCollector()
    {
        log.debug("SecretsManagerRotationCollector created");
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
        log.debug("SecretsManagerRotationCollector.collect called");

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
                    if (Boolean.TRUE.equals(secret.rotationEnabled()))
                    {
                        Map<String, Object> attributes = new HashMap<>();
                        attributes.put("secretArn", secret.arn());
                        attributes.put("secretName", secret.name());
                        attributes.put("rotationEnabled", true);
                        attributes.put("rotationLambdaArn", secret.rotationLambdaARN());
                        attributes.put("lastRotatedDate", secret.lastRotatedDate() != null ? secret.lastRotatedDate().toString() : null);

                        if (secret.rotationRules() != null)
                        {
                            attributes.put("automaticallyAfterDays", secret.rotationRules().automaticallyAfterDays());
                            attributes.put("duration", secret.rotationRules().duration());
                            attributes.put("scheduleExpression", secret.rotationRules().scheduleExpression());
                        }

                        String rotationId = secret.arn() + "/rotation";

                        CloudEntity entity = CloudEntity.builder()
                            .entityIdentifier(EntityIdentifier.builder()
                                .id(rotationId)
                                .qualifiedResourceName(rotationId)
                                .build())
                            .entityType(ENTITY_TYPE)
                            .name(secret.name() + "/rotation")
                            .collectorContext(collectorContext)
                            .attributes(attributes)
                            .rawPayload(secret)
                            .collectedAt(Instant.now())
                            .build();
                        entities.add(entity);
                    }
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
            log.error("SecretsManagerRotationCollector error: {}", e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage(), e);
            throw new CollectorException("Failed to collect Secrets Manager rotations", e);
        }
        catch (Exception e)
        {
            log.error("SecretsManagerRotationCollector unexpected error", e);
            throw new CollectorException("Unexpected error collecting Secrets Manager rotations", e);
        }
    }
}
