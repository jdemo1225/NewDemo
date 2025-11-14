package com.alchemain.rx.init;

import javax.servlet.ServletContextEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceServletContextListener;
import com.alchemain.rx.internal.ReactorCore;

public class ApplicationContextListener extends GuiceServletContextListener {
	private Logger log = LoggerFactory.getLogger(ApplicationContextListener.class);

	@Override
	protected Injector getInjector() {
		Module[] m = new Module[] { new ServletModule(), new ReactorModule(), new MongoModule(), new SearchModule() };
		GuiceFactory.initialize(m);
		return GuiceFactory.injector();
	}

	@Override
	public void contextInitialized(ServletContextEvent event) {
		log.info("Initializing application context");
		super.contextInitialized(event);
	}

	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {
		log.info("Destroying application context");
		ReactorCore reactorCore = GuiceFactory.injector().getInstance(ReactorCore.class);
		reactorCore.shutdown();

		super.contextDestroyed(servletContextEvent);
	}
}
