package com.alchemain.rx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.bus.JsonProvider;

public class Notifications implements Constants {
	protected static final Logger log = LoggerFactory.getLogger(Notifications.class);

	enum Channel {
		WEB, EMAIL;
	}

	public static ObjectNode createWeb(String recipient, String type, JsonNode data) {
		return create(Channel.WEB.name(), recipient, type, data);
	}

	public static ObjectNode createEmail(String recipient, String type, JsonNode data) {
		return create(Channel.EMAIL.name(), recipient, type, data);
	}

	private static ObjectNode create(String channel, String recipient, String type, JsonNode data) {
		ObjectNode web = JsonProvider.INSTANCE.getMapper().createObjectNode();
		web.put(CHANNEL, channel);
		web.put(RECIPIENT, recipient);
		web.put(TYPE, type);
		web.put(DATA, data);

		return web;
	}
}
