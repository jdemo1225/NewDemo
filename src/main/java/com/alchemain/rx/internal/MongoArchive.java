package com.alchemain.rx.internal;

import javax.inject.Inject;
import javax.inject.Provider;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.init.MongoWrapper;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Constants;
import com.mongodb.WriteResult;
import com.alchemain.rx.bus.JsonProvider;

public class MongoArchive {
	private final Logger log = LoggerFactory.getLogger(MongoArchive.class);
	private static ObjectMapper mapper = JsonProvider.INSTANCE.getMapper();

	private static final String format = "audit_%s";

	private final Provider<MongoWrapper> jongoProvider;

	@Inject
	public MongoArchive(Provider<MongoWrapper> jongoProvider) {
		this.jongoProvider = jongoProvider;
	}

	public void create(ExecutionContext context, String entity, JsonNode data) throws Exception {
		Jongo jongo = jongoProvider.get().getJongo(context.getTenant());
		String archive = String.format(format, entity);

		MongoCollection collection = jongo.getCollection(archive);

		log.debug("Adding entity: {} to archive", archive);

		ObjectNode payload = mapper.createObjectNode();
		payload.put(Constants._ID, data.get(Constants._ID));
		payload.putArray("states").add(data);

		collection.insert(payload);
	}

	public void append(ExecutionContext context, String entity, JsonNode data) throws Exception {
		Jongo jongo = jongoProvider.get().getJongo(context.getTenant());
		String archive = String.format(format, entity);

		MongoCollection collection = jongo.getCollection(archive);

		log.debug("Appending entity: {} changes to archive", archive);

		ObjectNode query = mapper.createObjectNode();
		query.put(Constants._ID, data.get(Constants._ID));

		ObjectNode update = mapper.createObjectNode();
		update.putObject("$push").put("states", data);

		String queryString = mapper.writeValueAsString(query);
		String updateString = mapper.writeValueAsString(update);

		WriteResult result = collection.update(queryString).with(updateString);

		log.debug("Updated {} documents", result.getN());
	}
}
