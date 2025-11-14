package com.alchemain.rx.init;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public final class GuiceFactory {
	private final static Logger log = LoggerFactory.getLogger(GuiceFactory.class);

	private static final String ALREADY_INITIALIZED = "Guice Injector already initialized. Attempt to re-initialize probably due to a bug";
	private static Injector INJECTOR;

	private GuiceFactory() {
	}

	public static void initialize(Module[] modules) {
		if (GuiceFactory.INJECTOR != null)
			throw new RuntimeException(ALREADY_INITIALIZED);

		log.info("Initializing google guice injector");
		Injector injector = Guice.createInjector(modules);
		GuiceFactory.INJECTOR = injector;
	}

	public static Injector injector() {
		return GuiceFactory.INJECTOR;
	}
}
