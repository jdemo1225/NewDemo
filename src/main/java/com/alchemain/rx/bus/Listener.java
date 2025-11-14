package com.alchemain.rx.bus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

public class Listener {
	@JsonProperty(value = "_id")
	private String id;

	private String topic;
	private String callback;
	private JsonNode dataPolicy;
	private JsonNode expansion;
	private JsonNode config;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getCallback() {
		return callback;
	}

	public void setCallback(String callback) {
		this.callback = callback;
	}

	public JsonNode getDataPolicy() {
		return dataPolicy;
	}

	public void setDataPolicy(JsonNode dataPolicy) {
		this.dataPolicy = dataPolicy;
	}
	
	public JsonNode getExpansion() {
		return expansion;
	}
	
	public void setExpansion(JsonNode expansion) {
		this.expansion = expansion;
	}
	
	public JsonNode getConfig() {
		return config;
	}
	
	public void setConfig(JsonNode config) {
		this.config = config;
	}

	public boolean matches(JsonNode data) throws Exception {
		String topic = data.get("topic").asText();

		if (this.topic.equals("*") || this.topic.equals(topic)) {
			return true;
		}
		return false;
	}

	@Override
	public boolean equals(Object that) {
		return this.id.equals(((Listener) that).id);
	}
}
