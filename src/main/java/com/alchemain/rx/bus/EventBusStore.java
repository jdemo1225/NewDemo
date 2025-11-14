package com.alchemain.rx.bus;

import java.util.Calendar;

import javax.inject.Inject;
import javax.inject.Provider;

import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.init.MongoWrapper;
import com.alchemain.rx.messages.Work;
import com.alchemain.rx.utils.Constants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

/**
 * This utility class deals with work state persistence to mongodb. Each work
 * item is first saved and then read back before delegated to an actor. This
 * ensures there is a complete work log created.
 * 
 * NOTE: Configured as a singleton in MongoModule class
 * 
 * @author bilal
 * 
 */

// TODO: clean up this code
public class EventBusStore {
	private final Logger log = LoggerFactory.getLogger(EventBusStore.class);

	private Provider<MongoWrapper> jongoProvider;
	private ObjectMapper mapper = JsonProvider.INSTANCE.getMapper();

	@Inject
	public EventBusStore(Provider<MongoWrapper> jongoProvider) {
		this.jongoProvider = jongoProvider;
	}

	public String storeListener(Work work) {
		JsonNode state = JsonProvider.INSTANCE.getMapper().convertValue(work, ObjectNode.class);
		return storeState(work.getContext().getTenant(), state);
	}

	public String storeState(String tenant, JsonNode state) {
		ObjectNode document = (ObjectNode) state;

		if (state.has(Constants._ID)) {
			document.remove(Constants._ID);
		}

		document.put(Constants.CREATE_TIME, Calendar.getInstance().getTimeInMillis());

		jongoProvider.get().getJongo(tenant).getCollection(Constants.JOBS).insert(state);
		return state.get(Constants._ID).asText();
	}

	public JsonNode readState(String tenant, String id) {
		DBObject query = new BasicDBObject().append(Constants._ID, id);
		JsonNode doc = jongoProvider.get().getJongo(tenant).getCollection(Constants.JOBS).findOne(query.toString())
				.as(JsonNode.class);
		return doc;
	}

	public JsonNode updateFork(String tenant, String forkerId, JsonNode state) throws Exception {
		ObjectNode document = (ObjectNode) state;

		if (state.has(Constants._ID)) {
			document.remove(Constants._ID);
		}

		document.put(Constants.CREATE_TIME, Calendar.getInstance().getTimeInMillis());

		MongoCollection jobs = jongoProvider.get().getJongo(tenant).getCollection(Constants.JOBS);

		ObjectNode query = mapper.createObjectNode();
		query.put(Constants._ID, forkerId);
		query.put(Constants.DATA_ITERATIONS_REMAINING, mapper.createObjectNode().put("$gt", 0));

		ObjectNode update = mapper.createObjectNode();

		ObjectNode push = update.putObject("$push");
		push.put(Constants.DATA_ITERATIONS_RESULT, state.get(Constants.DATA));

		ObjectNode dec = update.putObject("$inc");
		dec.put(Constants.DATA_ITERATIONS_REMAINING, -1);

		if (log.isTraceEnabled())
			log.trace("Updating fork state with query: {} and update: {}", query, update);
		else
			log.debug("Updating fork state id: {}", forkerId);

		String queryString = mapper.writeValueAsString(query);
		String updateString = mapper.writeValueAsString(update);

		return jobs.findAndModify(queryString).with(updateString).returnNew().as(JsonNode.class);
	}

	public JsonNode readLog(String tenant, String id) {
		QueryBuilder query = QueryBuilder.start();
		query.or(new BasicDBObject(Constants._ID, id), new BasicDBObject(Constants.PROCESS_ID, id), new BasicDBObject(
				Constants.FAILED_STATE_ID, id));

		BasicDBObject sort = new BasicDBObject("createTime", 1);

		MongoCollection collection = jongoProvider.get().getJongo(tenant).getCollection(Constants.JOBS);

		Iterable<JsonNode> log = collection.find(query.get().toString()).sort(sort.toString()).as(JsonNode.class);

		ObjectNode response = mapper.createObjectNode();
		ArrayNode entries = response.putArray("entries");
		for (JsonNode entry : log) {
			if (!Constants.DEBUG)
				((ObjectNode) entry).remove(Constants.CONTEXT);
			entries.add(entry);
		}

		return response;
	}
}
