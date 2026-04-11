package io.coherity.estoria.collector.provider.aws;

import io.coherity.estoria.collector.spi.ProviderContext;
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.organizations.model.DescribeOrganizationResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

public final class AwsExecutionGuards
{
    private AwsExecutionGuards()
    {
        // utility class
    }

    public static AccountScope resolveAccountScope(ProviderContext providerContext)
    {
        String currentAccountId = resolveCurrentAccountId(providerContext);

        String managementAccountId = null;
        boolean orgAccessible = false;

        try
        {
            OrganizationsClient organizationsClient =
                AwsClientFactory.getInstance().getOrganizationsClient(providerContext);

            DescribeOrganizationResponse response =
                organizationsClient.describeOrganization();

            managementAccountId = response.organization().masterAccountId();
            orgAccessible = true;
        }
        catch (Exception e)
        {
            // Organizations not accessible (common case for member accounts without permissions)
            // Treat as non-management account
        }

        if (!orgAccessible || managementAccountId == null)
        {
            return AccountScope.MEMBER_ACCOUNT;
        }

        if (currentAccountId.equals(managementAccountId))
        {
            return AccountScope.MANAGEMENT_ACCOUNT;
        }

        return AccountScope.MEMBER_ACCOUNT;
    }

    private static String resolveCurrentAccountId(ProviderContext providerContext)
    {
        StsClient stsClient =
            AwsClientFactory.getInstance().getStsClient(providerContext);

        GetCallerIdentityResponse response =
            stsClient.getCallerIdentity();

        return response.account();
    }
}
