package io.coherity.estoria.collector.provider.aws;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorCursor;
import io.coherity.estoria.collector.spi.CollectorException;
import io.coherity.estoria.collector.spi.CollectorRequest;
import io.coherity.estoria.collector.spi.CursorMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestCollector implements Collector
{
	private static final String PROVIDER_ID = "aws";
	private static final String ENTITY_TYPE = "AwsTestEntity";

	public TestCollector()
	{
		log.debug("TestCollector.TestCollector creating test collector");
	}
	
	@Override
	public String getProviderId()
	{
		log.debug("TestCollector.getProviderId called - returning {}", PROVIDER_ID);
		return PROVIDER_ID;
	}

	@Override
	public String getEntityType()
	{
		log.debug("TestCollector.getEntityType called - returning {}", ENTITY_TYPE);
		return ENTITY_TYPE;
	}

	@Override
	public Set<String> requiresEntityTypes()
	{
		log.debug("TestCollector.requiresEntityTypes called - no dependencies");
		return Set.of();
	}

	@Override
	public Set<String> getTags()
	{
		log.debug("TestCollector.getTags called");
		return Set.of("test", "aws");
	}

	@Override
	public CollectorCursor collect(CollectorRequest request) throws CollectorException
	{
		log.debug("TestCollector.collect called with request: {}", request);

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