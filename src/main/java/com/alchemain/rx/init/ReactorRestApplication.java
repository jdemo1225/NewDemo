package com.alchemain.rx.init;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;
import javax.ws.rs.ext.Provider;

import com.alchemain.rx.rest.JobsService;
import com.alchemain.rx.rest.ListenersService;

@Provider
public class ReactorRestApplication extends Application {

	@Override
	public Set<Class<?>> getClasses() {
		Set<Class<?>> classes = new HashSet<Class<?>>();
		classes.add(ResourceExceptionMapper.class);

		classes.add(JobsService.class);
		classes.add(ListenersService.class);

		return classes;
	}
}
