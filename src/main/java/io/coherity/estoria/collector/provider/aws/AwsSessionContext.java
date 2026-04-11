package io.coherity.estoria.collector.provider.aws;

import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.regions.Region;

@Value
@Builder
public class AwsSessionContext
{
    String profile;
    Region region;
    String currentAccountId;
    String managementAccountId;
    boolean organizationAccessible;
    AccountScope accountScope;
}