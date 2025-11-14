package com.alchemain.rx.init;

import akka.actor.ActorSystem;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.alchemain.rx.bus.ListenerCache;
import com.alchemain.rx.internal.ReactorCore;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ReactorModule extends AbstractModule {

	public ReactorModule() {
	}

	@Override
	protected void configure() {
		// Register directly invokable delegates.
		bind(DelegateRegistry.class).toInstance(new DelegateRegistry() {
			@Override
			public void initialize() throws RuntimeException {
			}
		});

		// Create actor system guice bindings.
		bind(ActorSystem.class).toProvider(new Provider<ActorSystem>() {
			@Override
			public ActorSystem get() {
				Config config = ConfigFactory.load();
				return ActorSystem.create("reactor", config);
			}
		}).in(Scopes.SINGLETON);
		bind(ReactorCore.class).in(Scopes.SINGLETON);
		bind(ListenerCache.class).in(Scopes.SINGLETON);
	}
}
