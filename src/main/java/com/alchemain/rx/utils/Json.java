package com.alchemain.rx.utils;

import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;

public class Json {

	public static String toString(JsonNode node) {
		if (node == null)
			return null;
		if (node.isMissingNode() || node.isNull())
			return null;
		return node.asText();
	}

	public static Date toDate(JsonNode node) {
		if (node == null)
			return null;
		if (node.isMissingNode() || node.isNull())
			return null;
		return new Date(node.asLong());
	}
}
