package com.alchemain.rx.init;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alchemain.rx.delegates.Delegate;

/**
 * Delegate registry. Only register directly invokable delegates (i.e. first
 * steps in larger chain). The simple name of the class is used as a key.
 * Delegates in different packages can share the same simple name but that will
 * still cause registration to fail. Registry is initialized in {@link
 * ReactorModule.class}, when creating Guice bindings.
 * 
 * @author bilal
 * 
 */

public class DelegateRegistry {
	private final Logger log = LoggerFactory.getLogger(DelegateRegistry.class);

	private Map<String, Class<? extends Delegate>> delegates;

	DelegateRegistry() {
		log.debug("Initializing delegate registry");
		this.delegates = new HashMap<String, Class<? extends Delegate>>();
		initialize();
	}

	public void register(Class<? extends Delegate> delegate) throws RuntimeException {
		if (delegates.containsKey(delegate.getSimpleName()))
			throw new RuntimeException("Delegate already registered");

		delegates.put(delegate.getSimpleName(), delegate);
	}

	public Class<? extends Delegate> get(String simpleName) throws RuntimeException {
		Class<? extends Delegate> delegate = delegates.get(simpleName);

		if (delegate == null)
			throw new RuntimeException("No delegate is registered with this name: " + simpleName);

		return delegate;
	}

	/**
	 * Empty method to be overridden in initialization code.
	 * 
	 * @throws RuntimeException
	 */
	public void initialize() throws RuntimeException {
	}
}
