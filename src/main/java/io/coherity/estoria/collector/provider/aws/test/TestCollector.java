package io.coherity.estoria.collector.provider.aws.test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.network.VpcCollector;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRequestParams;
import io.coherity.estoria.collector.spi.CursorMetadata;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	public static final String ENTITY_TYPE = "AwsTestEntity";
	
	private final CollectorInfo collectorInfo = 
			CollectorInfo
				.builder()
				.providerId(PROVIDER_ID)
				.entityType(ENTITY_TYPE)
				.requiredEntityTypes(Set.of(VpcCollector.ENTITY_TYPE))
				.tags(Set.of("test", "aws"))
				.build();

	
	public TestCollector()
	{
		log.debug("TestCollector.TestCollector creating test collector");
	}
	
	
	@Override
	public CollectorInfo getCollectorInfo()
	{
		log.debug("TestCollector.getCollectorInfo called - returning {}", this.collectorInfo);
		return this.collectorInfo;
	}
	
	@Override
	public CollectorCursor collect(ProviderContext providerContext, CollectorContext collectorContext, CollectorRequestParams request) throws CollectorException
	{
		log.debug("TestCollector.collect called with providerContext: {}, collectorContext: {}, request: {}", providerContext, collectorContext, request);

		return new CollectorCursor()
		{
			@Override
			public List<CloudEntity> getEntities()
			{
				log.debug("TestCollector.CollectorCursor.getEntities called - returning empty list");
				return List.of();
			}

			@Override
			public Optional<String> getNextCursorToken()
			{
				log.debug("TestCollector.CollectorCursor.getNextCursorToken called - no further pages");
				return Optional.empty();
			}

			@Override
			public CursorMetadata getMetadata()
			{
				log.debug("TestCollector.CollectorCursor.getMetadata called - returning empty metadata");
				return CursorMetadata.builder()
					.values(Map.of())
					.build();
			}
		};
	}
}