package com.alchemain.rx.utils;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class PropertiesUtil {
	private final static Logger log = LoggerFactory.getLogger(PropertiesUtil.class);
	private static Map<String, String> UNMODIFIABLE_MAP;

	static {
		try {
			log.info("Initializing Reactor web application properties");
			InputStream stream = PropertiesUtil.class.getClassLoader().getResourceAsStream("reactor.properties");
			Properties props = new Properties();
			props.load(stream);

			Map<String, String> temp = new HashMap<String, String>();

			for (Entry<Object, Object> entry : props.entrySet()) {
				temp.put(entry.getKey().toString(), entry.getValue().toString());
			}

			UNMODIFIABLE_MAP = Collections.unmodifiableMap(temp);
		} catch (Exception e) {
			throw new RuntimeException("Missing reactor.properties file. Make sure the file is on the classpath", e);
		}
	}

	public static String string(String key) {
		return UNMODIFIABLE_MAP.get(key);
	}

	public static String string(String key, String or) {
		if (Strings.isNullOrEmpty(UNMODIFIABLE_MAP.get(key)))
			return or;
		return UNMODIFIABLE_MAP.get(key);
	}

	public static int integer(String key, int or) {
		if (Strings.isNullOrEmpty(UNMODIFIABLE_MAP.get(key))) {
			return or;
		}
		return Integer.parseInt(UNMODIFIABLE_MAP.get(key));
	}

	public static boolean bool(String key, boolean or) {
		if (Strings.isNullOrEmpty(UNMODIFIABLE_MAP.get(key))) {
			return or;
		}
		return Boolean.parseBoolean(UNMODIFIABLE_MAP.get(key));
	}

	public static boolean bool(String key) {
		return Boolean.parseBoolean(UNMODIFIABLE_MAP.get(key));
	}

	public static int integer(String key) {
		return Integer.parseInt(UNMODIFIABLE_MAP.get(key));
	}
}
