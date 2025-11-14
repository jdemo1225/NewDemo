package com.alchemain.rx.internal.delegates;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.alchemain.rx.bus.Listener;
import com.alchemain.rx.bus.ListenerCache;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.utils.GraphityFetcher;

public class EventRouter extends Delegate {
	private final Logger log = LoggerFactory.getLogger(EventRouter.class);

	private ListenerCache cache;

	@Inject
	public EventRouter(ListenerCache cache) {
		this.cache = cache;
	}

	@Override
	public void execute() throws Exception {
		JsonNode data = getStartWork().getData();

		// Evaluate global listeners
		evaluate(cache.getGlobalCache(), data);

		// Evaluate tenant listeners
		if (!Constants.GLOBAL.equals(getStartWork().getContext().getTenant())) {
			evaluate(cache.getTenantCache(getStartWork().getContext()), data);
		}
	}

	private void evaluate(Iterable<Listener> tenantCache, JsonNode data) throws Exception {
		for (Listener listener : tenantCache) {
			if (listener.matches(data)) {
				JsonNode input = data.get("data");
				// if (listener.getDataPolicy() != null)
				// propagate(transformer.processObject(input,
				// listener.getDataPolicy()), listener);
				// else
				propagate((ObjectNode) input, listener);
			}
		}
	}

	private void propagate(ObjectNode data, Listener listener) throws Exception {
		if (!Strings.isNullOrEmpty(listener.getCallback())) {
			String callback = listener.getCallback();

			ObjectNode payload = getObjectMapper().createObjectNode();

			ObjectNode config = payload.with("config");
			config.put("endpoint", callback);
			config.put("listenerId", listener.getId());

			payload.put("payload", data);

			if (callback.startsWith("hero://")) {
				more(Hero.class, payload);
			} else if (callback.startsWith("https://")) {
				more(GenericCallback.class, payload);
			} else {
				log.warn("Listener callback: {} is not recognized, event is not routed", callback);
			}
		} else {
			log.warn("Listener doesn't specify a delegate or callback, event is not routed");
		}
	}
}
