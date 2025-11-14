package com.alchemain.rx.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.alchemain.rx.bus.JsonProvider;

public class ScuidResourceUtils implements Constants {
	private final static Pattern SELF_PATTERN = Pattern.compile("/(.*)/(.*)");

	public static String getAuthorization(String tenant) {
		String username = PropertiesUtil.string("scuid.agent.username");
		String password = PropertiesUtil.string("scuid.agent.password");

		username = String.format("%s+%s+%s", GLOBAL, tenant, username);
		return encode(username, password);
	}

	public static String encode(String username, String password) {
		String credential = username + ":" + password;
		byte[] credBytes = credential.getBytes();
		byte[] encodedCredBytes = Base64.encodeBase64(credBytes, false);
		return ("Basic " + new String(encodedCredBytes));
	}

	public static String parse(JsonNode data) {
		String self = data.path("self").asText();
		Matcher matcher = SELF_PATTERN.matcher(self);

		Preconditions.checkState(matcher.matches(), "data doesn't contain a valid self");

		return matcher.group(2);
	}

	public static void main(String[] args) {
		ObjectNode node = JsonProvider.INSTANCE.getMapper().createObjectNode();
		node.put(SELF, "/accounts/ccb26714-f3a0-42f5-99bf-9a7db5d0067d");
		System.out.println(parse(node));
	}
}
