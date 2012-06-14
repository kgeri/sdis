package org.ogreg.sdis.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic properties holder which supports defaults.
 * 
 * @author gergo
 */
public class Properties {
	private Map<String, Object> values = new HashMap<String, Object>(5);

	public void put(String key, Object value) {
		values.put(key, value);
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String key, T defaultValue, Class<T> expectedType) {
		Object value = values.get(key);
		if (expectedType.isInstance(value)) {
			return (T) value;
		} else {
			return defaultValue;
		}
	}
}
