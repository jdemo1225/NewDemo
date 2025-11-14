package com.alchemain.rx.init;

import javax.inject.Inject;

import org.bson.types.ObjectId;
import org.jongo.Jongo;
import org.jongo.Mapper;
import org.jongo.ObjectIdUpdater;
import org.jongo.marshall.jackson.JacksonMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.alchemain.rx.utils.Constants;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class MongoWrapper {
	private static String GUID_PATTERN = "^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$";

	private MongoClient mongoClient;
	private Mapper mapper;

	@Inject
	public MongoWrapper(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
		this.mapper = new JacksonMapper.Builder().withObjectIdUpdater(new ObjectIdUpdater() {
			public boolean mustGenerateObjectId(Object pojo) {
				return !((ObjectNode) pojo).has("_id");
			}

			public Object getId(Object pojo) {
				return ((ObjectNode) pojo).get("_id");
			}

			public void setObjectId(Object target, ObjectId id) {
				((ObjectNode) target).put("_id", id.toString());
			}
		}).build();
	}

	public Jongo getJongo(String database) {
		/*
		 * FUTURE: Should mongo access become a bottleneck, these DB references
		 * could be cached via a ConcurrentMap<String,DB> to act as an
		 * application-level cache
		 */
		Preconditions.checkArgument(!Strings.isNullOrEmpty(database), "Database name is required");

		if (!database.equals(Constants.GLOBAL) && !database.matches(GUID_PATTERN))
			throw new IllegalArgumentException(database + " is not a valid mongo db name");

		DB db = mongoClient.getDB(database);
		db.setWriteConcern(WriteConcern.ACKNOWLEDGED);

		Jongo jongo = new Jongo(db, mapper);
		return jongo;
	}
}
