package io.coherity.estoria.collector.provider.aws;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderException;
import io.coherity.estoria.collector.spi.ProviderSession;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AWSCloudProvider extends CloudProvider
{
	private static final String PROVIDER_ID = "aws";
	private static final String VERSION = "0.1.0";
	private static final String PROVIDER_NAME = "aws-provider";

	public AWSCloudProvider()
	{
		super(PROVIDER_ID, VERSION, PROVIDER_NAME, null);
		log.debug("AWSCloudProvider.AWSCloudProvider creating AWSCloudProvider");
	}

	@Override
	public ProviderSession openSession(ProviderContext context) throws ProviderException
	{
		log.debug("AWSCloudProvider.openSession opening session");
		return null;
	}
}
