package com.alchemain.rx.utils;

import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;

public interface Fetcher {
	public JsonNode fetch(JsonNode entity, Entry<String, JsonNode> field) throws Exception;
}
