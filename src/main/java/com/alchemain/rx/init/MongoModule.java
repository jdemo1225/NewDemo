package com.alchemain.rx.init;

import java.util.ArrayList;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.alchemain.rx.internal.WorkStateStore;
import com.alchemain.rx.utils.PropertiesUtil;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public final class MongoModule extends AbstractModule {
	private final Logger log = LoggerFactory.getLogger(MongoModule.class);

	protected static final String MONGODB_HOST_PROP = "mongodb.host";
	protected static final String MONGODB_SSL_PROP = "mongodb.ssl";

	@Override
	protected void configure() {
		bind(MongoClient.class).toInstance(mongoConnect());
		bind(MongoWrapper.class).in(Scopes.SINGLETON);
		bind(WorkStateStore.class).in(Scopes.SINGLETON);
	}

	public MongoClient mongoConnect() {
		try {
			boolean ssl = PropertiesUtil.bool(MONGODB_SSL_PROP);
			String hosts = PropertiesUtil.string(MONGODB_HOST_PROP);

			ArrayList<ServerAddress> addr = new ArrayList<ServerAddress>();
			for (String host : hosts.split(",")) {
				addr.add(new ServerAddress(host));
			}

			log.info("Initializing mongo data store with replica set: {}", addr.toString());
			MongoClient mongoClient;
			if (ssl) {
				MongoClientOptions opts = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault())
						.build();
				mongoClient = new MongoClient(addr, opts);
			} else {
				mongoClient = new MongoClient(addr);
			}

			return mongoClient;
		} catch (Exception e) {
			throw new RuntimeException("Unable to initialize mongo data store", e);
		}
	}
}
