package com.alchemain.rx.bus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Inject;
import javax.inject.Provider;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.alchemain.rx.init.MongoWrapper;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Constants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class ListenerCache {
	private final Logger log = LoggerFactory.getLogger(ListenerCache.class);
	private static ObjectMapper mapper = JsonProvider.INSTANCE.getMapper();

	private final Map<String, Map<String, Listener>> cacheCache;
	private final Provider<MongoWrapper> jongoProvider;

	@Inject
	public ListenerCache(Provider<MongoWrapper> jongoProvider) {
		this.jongoProvider = jongoProvider;
		this.cacheCache = new ConcurrentHashMap<String, Map<String, Listener>>();

		try {
			Map<String, Listener> globalCache = new ConcurrentHashMap<String, Listener>();
			globalCache.putAll(new Loader(Constants.GLOBAL).call());

			this.cacheCache.put(Constants.GLOBAL, globalCache);
		} catch (Throwable t) {
			Throwables.propagate(t);
		}
	}

	public Listener get(ExecutionContext context, String id) throws Exception {
		return getCache(context.getTenant()).get(id);
	}

	public void put(ExecutionContext context, JsonNode listener) throws Exception {
		Jongo jongo = jongoProvider.get().getJongo(context.getTenant());
		MongoCollection collection = jongo.getCollection("listeners");

		if (listener.has(Constants._ID)) {
			log.debug("Updating a listeners");
			collection.getDBCollection().save(mapper.convertValue(listener, BasicDBObject.class));
		} else {
			log.debug("Creating a listeners");
			collection.insert(listener);
		}

		Map<String, Listener> cache = getCache(context.getTenant());
		cache.put(listener.get(Constants._ID).asText(), mapper.convertValue(listener, Listener.class));
	}

	public void remove(ExecutionContext context, String id) {
		log.debug("Removing a listeners");

		Jongo jongo = jongoProvider.get().getJongo(context.getTenant());
		MongoCollection collection = jongo.getCollection("listeners");

		DBObject query = new BasicDBObject().append(Constants._ID, id);
		collection.remove(query.toString());

		getCache(context.getTenant()).remove(id);
	}

	public Iterable<Listener> getGlobalCache() {
		return getCache(Constants.GLOBAL).values();
	}

	public Iterable<Listener> getTenantCache(ExecutionContext context) {
		return getCache(context.getTenant()).values();
	}

	private Map<String, Listener> getCache(String tenant) {
		Map<String, Listener> cache = cacheCache.get(tenant);

		if (cache == null) {
			try {
				cache = new ConcurrentHashMap<String, Listener>();
				cache.putAll(new Loader(tenant).call());
				cacheCache.put(tenant, cache);
			} catch (Throwable t) {
				Throwables.propagate(t);
			}
		}

		return cache;
	}

	class Loader implements Callable<Map<String, Listener>> {
		private String tenant;

		public Loader(String tenant) {
			this.tenant = tenant;
		}

		@Override
		public Map<String, Listener> call() throws Exception {
			log.debug("Fetching listeners for: {}", tenant);

			Map<String, Listener> listeners = new HashMap<String, Listener>();

			Jongo jongo = jongoProvider.get().getJongo(tenant);
			Iterable<Listener> result = jongo.getCollection("listeners").find().as(Listener.class);

			for (Listener listener : result) {
				listeners.put(listener.getId(), listener);
			}

			return listeners;
		}
	}
}
