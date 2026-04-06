package io.coherity.estoria.collector.provider.aws;

import java.util.Map;

import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.EndpointResolver;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderException;
import io.coherity.estoria.collector.spi.ProviderSession;
import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@Getter
@Builder
public class AwsProviderSession implements ProviderSession
{
    private final CloudProvider provider;
    private final ProviderContext providerContext;
    private final String profile;
    private final String region;
    private final Map<String, String> attributes;
    private final AwsCredentialsProvider credentialsProvider;
    private final AwsClientFactory clientFactory;

    @Override
    public void close()
    {
        if (this.clientFactory != null)
        {
            this.clientFactory.shutdown();
        }
    }

	@Override
	public EndpointResolver getEndpointResolver()
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getService(Class<T> serviceType) throws ProviderException
	{
		// TODO Auto-generated method stub
		return null;
	}
}