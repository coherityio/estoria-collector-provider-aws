package io.coherity.estoria.collector.provider.aws;

public enum EndpointScope
{
	REGIONAL,
	GLOBAL,
	FIXED_REGION;

	boolean requiresRegion()
	{
		return this == REGIONAL;
	}

	String effectiveRegion(String requestedRegion, String fixedRegion)
	{
		if (this == FIXED_REGION)
		{
			return fixedRegion;
		}
		return requestedRegion;
	}
}
