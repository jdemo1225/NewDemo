package com.alchemain.rx.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.bus.JsonProvider;

public class JsonMerger {
	public static ObjectNode object(JsonNode policy, ObjectNode base, JsonNode addon) throws Exception {
		Iterator<Entry<String, JsonNode>> fields = addon.fields();

		while (fields.hasNext()) {
			Entry<String, JsonNode> field = fields.next();

			if (field.getValue().isObject()) {
				base.set(field.getKey(), object(policy, base.with(field.getKey()), field.getValue()));
			} else if (field.getValue().isArray()) {
				base.set(field.getKey(), array((ArrayNode) base.get(field.getKey()), (ArrayNode) field.getValue()));
			} else {
				List<JsonNode> sorted = sort(policy.path(field.getKey()));

				if (sorted != null && (sorted.indexOf(field.getKey()) > sorted.indexOf(base.path(field.getKey()))))
					base.set(field.getKey(), field.getValue());
			}
		}

		return base;
	}

	public static ArrayNode array(ArrayNode base, ArrayNode addon) throws Exception {
		ArrayNode array = JsonProvider.INSTANCE.getMapper().createArrayNode();

		if (base != null) {
			array.addAll((ArrayNode) base);
		}

		for (JsonNode item : addon) {
			array.add(item);
		}

		return array;
	}

	private static List<JsonNode> sort(JsonNode list) {
		if (list.isMissingNode())
			return null;

		List<JsonNode> sorted = new ArrayList<JsonNode>(list.size());

		for (JsonNode item : list) {
			sorted.add(item);
		}
		return sorted;
	}
}
