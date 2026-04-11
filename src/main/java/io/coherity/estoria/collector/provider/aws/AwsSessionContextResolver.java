package io.coherity.estoria.collector.provider.aws;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.DescribeOrganizationResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

/**
 * Resolves and caches AWS session context derived from the current authenticated
 * provider context.
 *
 * Cache key is based on the configured profile and region from ProviderContext.
 * This is sufficient for the current profile-driven execution model.
 */
@Slf4j
public class AwsSessionContextResolver
{
    private static final String PROVIDER_CONTEXT_ATTRIBUTE_PROFILE = "profile";
    private static final String PROVIDER_CONTEXT_ATTRIBUTE_REGION = "region";
    private static final String DEFAULT_PROFILE = "default";
    private static final String CACHE_KEY_SEPARATOR = ":";

    private static final String COLLECTOR_CONTEXT_ATTRIBUTE_CONTAINMENT_SCOPE = "containment-scope";
    private static final String COLLECTOR_CONTEXT_ATTRIBUTE_INCLUDE_REFERENCE  = "include-reference";

    private static final AwsSessionContextResolver INSTANCE = new AwsSessionContextResolver();

    private final Map<String, AwsSessionContext> sessionContextCache = new ConcurrentHashMap<>();

    public static AwsSessionContextResolver getInstance()
    {
        return INSTANCE;
    }

    private AwsSessionContextResolver() {}

    public AwsSessionContext resolveSessionContext(ProviderContext providerContext, CollectorContext collectorContext)
    {
        Validate.notNull(providerContext, "required: providerContext");

        String cacheKey = buildCacheKey(providerContext);

        return this.sessionContextCache.computeIfAbsent(
            cacheKey, key -> this.loadSessionContext(providerContext));
    }

    public AccountScope resolveAccountScope(ProviderContext providerContext)
    {
        return this.resolveSessionContext(providerContext, null).getAccountScope();
    }

    public void evict(ProviderContext providerContext)
    {
        Validate.notNull(providerContext, "required: providerContext");
        this.sessionContextCache.remove(buildCacheKey(providerContext));
    }

    public void clear()
    {
        this.sessionContextCache.clear();
    }

    protected AwsSessionContext loadSessionContext(ProviderContext providerContext)
    {
        String profile = resolveProfile(providerContext);
        Region region = resolveRegion(providerContext);

        String currentAccountId = resolveCurrentAccountId(providerContext);

        String managementAccountId = null;
        boolean organizationAccessible = false;
        AccountScope accountScope = AccountScope.MEMBER_ACCOUNT;

        try
        {
            OrganizationsClient organizationsClient =
                AwsClientFactory.getInstance().getOrganizationsClient(providerContext);

            DescribeOrganizationResponse response = organizationsClient.describeOrganization();
            managementAccountId = response.organization().masterAccountId();
            organizationAccessible = true;

            if (StringUtils.isNotBlank(currentAccountId)
                && StringUtils.isNotBlank(managementAccountId)
                && currentAccountId.equals(managementAccountId))
            {
                accountScope = AccountScope.MANAGEMENT_ACCOUNT;
            }
        }
        catch (Exception e)
        {
            // Best effort only.
            // If Organizations cannot be queried, assume member account.
            organizationAccessible = false;
            accountScope = AccountScope.MEMBER_ACCOUNT;
        }

        return AwsSessionContext.builder()
            .profile(profile)
            .region(region)
            .currentAccountId(currentAccountId)
            .managementAccountId(managementAccountId)
            .organizationAccessible(organizationAccessible)
            .accountScope(accountScope)
            .build();
    }

    protected String resolveCurrentAccountId(ProviderContext providerContext)
    {
        StsClient stsClient = AwsClientFactory.getInstance().getStsClient(providerContext);
        GetCallerIdentityResponse response = stsClient.getCallerIdentity();
        return response.account();
    }

    protected static ContainmentScope resolveContainmentScope(CollectorContext collectorContext)
    {
        if (collectorContext != null && collectorContext.getAttributes() != null)
        {
            Object found = collectorContext.getAttributes().get(COLLECTOR_CONTEXT_ATTRIBUTE_CONTAINMENT_SCOPE);
            if (found != null)
            {
                String value = found.toString().trim().toUpperCase();
                try
                {
                    return ContainmentScope.valueOf(value);
                }
                catch (IllegalArgumentException e)
                {
                    // unrecognised value — fall through to null
                }
            }
        }
        return null;
    }

    protected static boolean resolveIncludeReferenceCategory(CollectorContext collectorContext)
    {
        if (collectorContext != null && collectorContext.getAttributes() != null)
        {
            Object found = collectorContext.getAttributes().get(COLLECTOR_CONTEXT_ATTRIBUTE_INCLUDE_REFERENCE);
            if (found != null)
            {
                if (found instanceof Boolean)
                {
                    return (Boolean) found;
                }
                return Boolean.parseBoolean(found.toString().trim());
            }
        }
        return false;
    }

    protected static String buildCacheKey(ProviderContext providerContext)
    {
        String profile = resolveProfile(providerContext);
        Region region = resolveRegion(providerContext);

        String resolvedProfile = StringUtils.isNotBlank(profile) ? profile : DEFAULT_PROFILE;
        String resolvedRegion = region != null ? region.id() : "default";

        return resolvedProfile + CACHE_KEY_SEPARATOR + resolvedRegion;
    }

    protected static String resolveProfile(ProviderContext providerContext)
    {
        if (providerContext != null && providerContext.getAttributes() != null)
        {
            Object found = providerContext.getAttributes().get(PROVIDER_CONTEXT_ATTRIBUTE_PROFILE);
            if (found != null)
            {
                return found.toString();
            }
        }

        return DEFAULT_PROFILE;
    }

    protected static Region resolveRegion(ProviderContext providerContext)
    {
        if (providerContext != null && providerContext.getAttributes() != null)
        {
            Object found = providerContext.getAttributes().get(PROVIDER_CONTEXT_ATTRIBUTE_REGION);
            if (found != null)
            {
                String regionStr = found.toString();
                if (StringUtils.isNotBlank(regionStr))
                {
                    return Region.of(regionStr);
                }
            }
        }

        return null;
    }
}