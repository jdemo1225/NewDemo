package com.alchemain.rx.init;

import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class ServletModule extends JerseyServletModule {
	private static final String servletPath = "api/v1";

	@Override
	protected void configureServlets() {
		// bind providers

		// Configure servlets
		Map<String, String> params = new HashMap<String, String>();
		params.put("javax.ws.rs.Application", ReactorRestApplication.class.getName());
		params.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
		params.put("com.sun.jersey.config.feature.Trace", "true");
		serve("/" + servletPath + "/*").with(GuiceContainer.class, params);
	}
}
