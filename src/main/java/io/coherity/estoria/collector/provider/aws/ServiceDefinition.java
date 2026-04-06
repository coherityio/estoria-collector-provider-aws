package io.coherity.estoria.collector.provider.aws;

import java.net.URI;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ServiceDefinition
{
	private final String endpointPrefix;
	private final String signingName;
	private final EndpointScope endpointScope;
	private final String fixedRegion;

	public ServiceDefinition(String endpointPrefix, String signingName, EndpointScope endpointScope, String fixedRegion)
	{
		this.endpointPrefix = endpointPrefix;
		this.signingName = signingName;
		this.endpointScope = endpointScope;
		this.fixedRegion = fixedRegion;
	}

	public static ServiceDefinition regional(String endpointPrefix, String signingName)
	{
		return new ServiceDefinition(endpointPrefix, signingName, EndpointScope.REGIONAL, null);
	}

	public static ServiceDefinition global(String endpointPrefix, String signingName)
	{
		return new ServiceDefinition(endpointPrefix, signingName, EndpointScope.GLOBAL, null);
	}

	public static ServiceDefinition fixedRegion(String endpointPrefix, String signingName, String fixedRegion)
	{
		return new ServiceDefinition(endpointPrefix, signingName, EndpointScope.FIXED_REGION, fixedRegion);
	}

	public URI toUri(String region, String dnsSuffix)
	{
		if (this.endpointScope == EndpointScope.GLOBAL)
		{
			return URI.create("https://" + this.endpointPrefix + "." + dnsSuffix);
		}
		return URI.create("https://" + this.endpointPrefix + "." + region + "." + dnsSuffix);
	}
}