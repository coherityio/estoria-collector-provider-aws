package io.coherity.estoria.collector.provider.aws;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;

/**
 * Centralized factory for AWS SDK v2 clients used by collectors.
 *
 * Currently supports EC2 and caches clients per region to encourage reuse.
 */
@Slf4j
public class AwsClientFactory
{
	private static final String DEFAULT_REGION_KEY = "us-east-1";
	private static final AwsClientFactory INSTANCE = new AwsClientFactory();

	private final Map<String, Ec2Client> ec2ClientsByRegionMap = new ConcurrentHashMap<>();

	private AwsClientFactory()
	{
	}

	public static AwsClientFactory getInstance()
	{
		return INSTANCE;
	}

	/**
	 * Obtain a shared EC2 client for the region implied by the given scope.
	 *
	 * If scope.attributes["region"] is set, that region is used; otherwise the
	 * AWS SDK default region provider chain applies.
	 */
	public Ec2Client getEc2Client(ProviderContext providerContext)
	{
		String region = resolveRegion(providerContext);
		String cachedClientKey = (region != null && !region.isBlank()) ? region : DEFAULT_REGION_KEY;
	 	return ec2ClientsByRegionMap.computeIfAbsent(cachedClientKey, k -> AwsClientFactory.createEc2Client(region));
	}

	private static Ec2Client createEc2Client(String region)
	{
		ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
			.addExecutionInterceptor(new AwsHttpLoggingInterceptor())
			.build();

		if (region != null && !region.isBlank())
		{
			Region awsRegion = Region.of(region);
			log.debug("AwsClientFactory.createEc2Client creating EC2 client for region={}", awsRegion);
			return Ec2Client.builder()
				.region(awsRegion)
				.overrideConfiguration(overrideConfiguration)
				.build();
		}
		log.debug("AwsClientFactory.createEc2Client creating EC2 client using default region provider chain");
		return Ec2Client.builder()
			.overrideConfiguration(overrideConfiguration)
			.build();
	}

	private String resolveRegion(ProviderContext providerContext)
	{
		if (providerContext != null && providerContext.getAttributes() != null)
		{
			String region = providerContext.getAttributes().get("region").toString();
			if (region != null && !region.isBlank())
			{
				return region;
			}
		}
		return null;
	}

	/**
	 * Close and clear all cached clients. Intended for use during shutdown
	 * or tests; collectors themselves should not usually call this.
	 */
	public void shutdown()
	{
		for (Ec2Client client : ec2ClientsByRegionMap.values())
		{
			try
			{
				client.close();
			}
			catch (Exception e)
			{
				log.warn("AwsClientFactory.shutdown error while closing EC2 client", e);
			}
		}
		ec2ClientsByRegionMap.clear();
	}
}
