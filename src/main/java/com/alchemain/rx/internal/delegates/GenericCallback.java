package com.alchemain.rx.internal.delegates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.alchemain.rx.bus.Listener;
import com.alchemain.rx.bus.ListenerCache;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.utils.RestClientFactory;
import com.alchemain.rx.utils.RestClientFactory.RestResponse;

public class GenericCallback extends Delegate {
	private final Logger log = LoggerFactory.getLogger(GenericCallback.class);
	private ListenerCache cache;

	@Inject
	public GenericCallback(ListenerCache cache) {
		this.cache = cache;
	}

	@Override
	public void execute() throws Exception {
		ObjectNode data = (ObjectNode) getStartWork().getData();

		JsonNode config = data.get("config");
		JsonNode payload = data.get("payload");

		String uri = config.path("endpoint").asText();
		String listenerId = config.path("listenerId").asText();

		ExecutionContext context = getStartWork().getContext();
		
		Listener listener = cache.get(context, listenerId);

		log.debug("Routing request to callback: {}", uri);

		RestResponse response = RestClientFactory.postResource(context, uri, listener.getConfig(), payload);

		response.discardIfSuccessElse(Errors.restPostFailed(uri));
	}
}
