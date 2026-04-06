package io.coherity.estoria.collector.provider.aws;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderException;
import io.coherity.estoria.collector.spi.ProviderSession;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

@Slf4j
public class AWSCloudProvider extends CloudProvider
{
	private static final String PROVIDER_ID = "aws";
	private static final String VERSION = "0.1.0";
	private static final String PROVIDER_NAME = "aws-provider";

	public AWSCloudProvider()
	{
		super(PROVIDER_ID, VERSION, PROVIDER_NAME, null);
		// super(PROVIDER_ID, VERSION, PROVIDER_NAME, null);
		log.debug("AWSCloudProvider.AWSCloudProvider creating AWSCloudProvider");
	}

	@Override
	public ProviderSession openSession(ProviderContext providerContext) throws ProviderException
	{
		if (providerContext == null)
		{
			throw new IllegalArgumentException("providerContext cannot be null");
		}

		Map<String, String> attributes = new HashMap<>();
		if (providerContext.getAttributes() != null)
		{
			Map<String, Object> providerContextConfigMap = providerContext.getAttributes();
			for (String key : providerContextConfigMap.keySet())
			{
				attributes.put(key, providerContextConfigMap.get(key).toString());
			}
		}

		String profile = firstNonBlank(attributes.get("profile"), System.getenv("AWS_PROFILE"),
				System.getProperty("aws.profile"));

		String region = firstNonBlank(attributes.get("region"), System.getenv("AWS_REGION"),
				System.getenv("AWS_DEFAULT_REGION"), System.getProperty("aws.region"));

		AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(profile);
		String resolvedRegion = resolveRegion(region);

		validateCredentials(credentialsProvider, resolvedRegion, profile);

		return AwsProviderSession.builder().provider(this).providerContext(providerContext).profile(profile)
				.region(resolvedRegion).attributes(Collections.unmodifiableMap(attributes))
				.credentialsProvider(credentialsProvider)
				//.clientFactory(AwsClientFactory.getInstance(credentialsProvider, resolvedRegion))
				.clientFactory(AwsClientFactory.getInstance())
				.build();
	}

	private AwsCredentialsProvider buildCredentialsProvider(String profile)
	{
		if (profile != null && !profile.isBlank())
		{
			return ProfileCredentialsProvider.builder().profileName(profile).build();
		}

		return DefaultCredentialsProvider.builder().build();
	}

	private String resolveRegion(String configuredRegion)
	{
		if (configuredRegion != null && !configuredRegion.isBlank())
		{
			return configuredRegion;
		}

		try
		{
			Region region = DefaultAwsRegionProviderChain.builder().build().getRegion();
			return region != null ? region.id() : null;
		} catch (Exception e)
		{
			return null;
		}
	}

	private void validateCredentials(AwsCredentialsProvider credentialsProvider, String region, String profile)
			throws ProviderException
	{
		try (StsClient sts = buildStsClient(credentialsProvider, region))
		{
			sts.getCallerIdentity();
			log.info("Opened AWS session using profile={} region={}", profile, region);
		} catch (Exception e)
		{
			throw new ProviderException("Failed to open AWS session. Ensure `aws sso login` has already been run"
					+ (profile != null ? " for profile '" + profile + "'" : ""), e);
		}
	}

	private StsClient buildStsClient(AwsCredentialsProvider credentialsProvider, String region)
	{
		StsClientBuilder builder = StsClient.builder().credentialsProvider(credentialsProvider);

		if (region != null && !region.isBlank())
		{
			builder.region(Region.of(region));
		}

		return builder.build();
	}

	private String firstNonBlank(String... values)
	{
		if (values == null)
		{
			return null;
		}

		for (String value : values)
		{
			if (value != null && !value.isBlank())
			{
				return value;
			}
		}
		return null;
	}
}
