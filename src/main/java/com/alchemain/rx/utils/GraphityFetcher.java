package com.alchemain.rx.utils;

import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.RestClientFactory.RestResponse;

public class GraphityFetcher implements Fetcher {
	private ExecutionContext context;

	public GraphityFetcher(ExecutionContext context) {
		this.context = context;
	}

	@Override
	public JsonNode fetch(JsonNode entity, Entry<String, JsonNode> field) throws Exception {
		JsonNode attribute = entity.path(field.getKey());

		StringBuilder base = new StringBuilder(PropertiesUtil.string("scuid.proxy.api.uri"));

		String uri = base.append(attribute.get("self").asText()).toString();

		RestResponse get = RestClientFactory.getScuidResource(context, uri.toString());

		return get.asJsonIfSuccessElse(Errors.restGetFailed(uri.toString()));
	}
}
