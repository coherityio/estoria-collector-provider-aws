package io.coherity.estoria.collector.provider.aws;

import java.util.*;
import java.util.stream.Collectors;

public enum CollectorContextAttributeKey
{
	CONTAINMENT_SCOPE, INCLUDE_REFERENCE, ENTITY_CATEGORY;

	private static final Map<String, CollectorContextAttributeKey> LOOKUP;

	static
	{
		LOOKUP = Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(k -> normalize(k.name()), k -> k));
	}

	public String key()
	{
		return this.name();
	}

	public String keyLowercase()
	{
		return this.name().toLowerCase().replace('_', '-');
	}

	public static Set<String> keys()
	{
		return Arrays.stream(values()).map(Enum::name).collect(Collectors.toUnmodifiableSet());
	}

	public static Set<String> keysLowercase()
	{
		return Arrays.stream(values()).map(CollectorContextAttributeKey::keyLowercase)
				.collect(Collectors.toUnmodifiableSet());
	}

	public static CollectorContextAttributeKey from(String value)
	{
		if (value == null || value.isBlank())
		{
			throw new IllegalArgumentException("Attribute key cannot be null or blank");
		}

		CollectorContextAttributeKey key = LOOKUP.get(normalize(value));

		if (key == null)
		{
			throw new IllegalArgumentException("Unknown CollectorContextAttributeKey: " + value);
		}

		return key;
	}

	private static String normalize(String input)
	{
		return input.trim().toUpperCase().replace('-', '_');
	}
}