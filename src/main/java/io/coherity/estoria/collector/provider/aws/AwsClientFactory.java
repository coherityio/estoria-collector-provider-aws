package io.coherity.estoria.collector.provider.aws;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;

@Slf4j
public class AwsClientFactory
{
	private static final String PROFILE_KEY = "profile";
	private static final String DEFAULT_PROFILE = "default";
	private static final String REGION_KEY = "region";
	private static final String DEFAULT_REGION = "default";
	
	private static final AwsClientFactory INSTANCE = new AwsClientFactory();

	private final Map<String, Ec2Client> ec2ClientsByRegionMap = new ConcurrentHashMap<>();

	private AwsClientFactory()
	{
	}

	public static AwsClientFactory getInstance()
	{
		return INSTANCE;
	}

	public Ec2Client getEc2Client(ProviderContext providerContext)
	{
		Validate.notNull(providerContext, "required: providerContext");
		
		String profile = AwsClientFactory.resolveProfile(providerContext);
		String region = AwsClientFactory.resolveRegion(providerContext);
		
		String cachedClientKey = (region != null && !region.isBlank()) ? region : DEFAULT_REGION;
		
	 	return ec2ClientsByRegionMap.computeIfAbsent(cachedClientKey, k -> AwsClientFactory.createEc2Client(profile, region));
	}

	private static Ec2Client createEc2Client(String profile, String region)
	{
		ClientOverrideConfiguration overrideConfiguration = 
				ClientOverrideConfiguration.builder()
					.addExecutionInterceptor(new AwsHttpLoggingInterceptor())
					.build();

		
		Ec2ClientBuilder builder = Ec2Client.builder().overrideConfiguration(overrideConfiguration);
		
		if (StringUtils.isNotBlank(profile))
		{
			builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
			log.debug("AwsClientFactory.createEc2Client using profile: {}", profile);
		}

		if (StringUtils.isNotBlank(region))
		{
			Region awsRegion = Region.of(region);
			log.debug("AwsClientFactory.createEc2Client using region: {}", awsRegion);
			builder.region(awsRegion);
		}
		else
		{
			log.debug("AwsClientFactory.createEc2Client using default region for profile");
		}
		
		return builder.build();
	}

	private static String resolveRegion(ProviderContext providerContext)
	{
		if (providerContext != null && providerContext.getAttributes() != null)
		{
			Object foundRegion = providerContext.getAttributes().get(REGION_KEY);
			if(foundRegion != null)
			{
				return foundRegion.toString();
			}
		}
		return null;
	}
	
	private static String resolveProfile(ProviderContext providerContext)
	{
		if (providerContext != null && providerContext.getAttributes() != null)
		{
			Object foundProfile = providerContext.getAttributes().get(PROFILE_KEY);
			if(foundProfile != null)
			{
				return foundProfile.toString();
			}
		}
		return DEFAULT_PROFILE;
	}
	

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
