package com.alchemain.rx.utils;

import java.util.Iterator;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Expander {
	private ObjectNode entity;
	private JsonNode expansion;
	private Fetcher fetcher;

	public static Expander expand(JsonNode entity) {
		return new Expander(entity);
	}

	public Expander(JsonNode entity) {
		this.entity = (ObjectNode) entity;
	}

	public ObjectNode entity() {
		return entity;
	}

	public Expander with(JsonNode expansion) {
		this.expansion = expansion;
		return this;
	}

	public JsonNode expansion() {
		return expansion;
	}

	public Expander using(Fetcher fetcher) {
		this.fetcher = fetcher;
		return this;
	}

	public Fetcher fetcher() {
		return fetcher;
	}

	public ObjectNode run() throws Exception {
		if (expansion.size() > 0) {
			Iterator<Entry<String, JsonNode>> fields = expansion.fields();
			while (fields.hasNext()) {
				Entry<String, JsonNode> field = fields.next();
				JsonNode attribute = entity.path(field.getKey());

				if (!attribute.isMissingNode()) {
					entity.with(field.getKey()).putAll((ObjectNode) fetcher.fetch(entity, field));
				}
			}
		}
		return entity;
	}
}
