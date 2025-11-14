package com.alchemain.rx.internal.delegates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.utils.PropertiesUtil;
import com.alchemain.rx.utils.RestClientFactory;
import com.alchemain.rx.utils.RestClientFactory.RestResponse;

public class Hero extends Delegate {
	private final Logger log = LoggerFactory.getLogger(Hero.class);

	@Override
	public void execute() throws Exception {
		String base = PropertiesUtil.string("hero.api.uri");

		ObjectNode data = (ObjectNode) getStartWork().getData();
		
		JsonNode config = data.get("config");
		JsonNode payload = data.get("payload");

		String endpoint = config.path("endpoint").asText();

		String uri = endpoint.replace("hero:/", base);

		log.debug("Routing request to callback: {}", uri);

		RestResponse response = RestClientFactory.postHeroResource(getStartWork().getContext(), uri, payload);

		response.discardIfSuccessElse(Errors.restPostFailed(uri));
	}
}
