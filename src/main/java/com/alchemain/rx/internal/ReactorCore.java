package com.alchemain.rx.internal;

import it.sauronsoftware.cron4j.Scheduler;

import javax.inject.Inject;
import javax.inject.Provider;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.init.DelegateRegistry;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.messages.Messages;

/**
 * NOTE: DON'T MODIFY ANY CODE HERE.
 * 
 * Brain of the actor system.
 * 
 * @author bilal
 * 
 */
public class ReactorCore {
	private final ActorSystem system;
	private final ActorRef supervisor;
	private final Provider<WorkStateStore> stateStore;
	private final DelegateRegistry registry;
	private final Scheduler scheduler;

	@Inject
	public ReactorCore(ActorSystem system, DelegateRegistry registry, Provider<WorkStateStore> stateStore) {
		this.scheduler = new Scheduler();
		this.system = system;
		this.registry = registry;
		this.stateStore = stateStore;

		SupervisorCreator creator = new SupervisorCreator(scheduler);

		this.supervisor = system.actorOf(Props.create(creator), "supervisor");

		this.scheduler.start();
	}

	public void tell(ExecutionContext context, Class<? extends Delegate> delegate, JsonNode data) throws Exception {
		supervisor.tell(Messages.moreWork(delegate, data).setContext(context), ActorRef.noSender());
	}

	public void tell(ExecutionContext context, String id) throws Exception {
		supervisor.tell(new ReadState(context, id), ActorRef.noSender());
	}

	public void restart(ExecutionContext context, String id, JsonNode data) throws Exception {
		supervisor.tell(new ReadState(context, id, data), ActorRef.noSender());
	}

	public void tell(ExecutionContext context, String delegate, JsonNode data) throws Exception {
		if (registry.get(delegate) == null)
			throw new IllegalArgumentException("Invalid delegate or it's not registered");

		tell(context, registry.get(delegate), data);
	}

	public String ask(ExecutionContext context, Class<? extends Delegate> delegate, JsonNode data) throws Exception {
		String id = stateStore.get().storeState(Messages.moreWork(delegate, data).setContext(context));
		supervisor.tell(new ReadState(context, id), ActorRef.noSender());
		return id;
	}

	public String ask(ExecutionContext context, String delegate, JsonNode data) throws Exception {
		if (registry.get(delegate) == null)
			throw new IllegalArgumentException("Invalid delegate or it's not registered");

		return ask(context, registry.get(delegate), data);
	}

	public JsonNode log(ExecutionContext context, String id) throws Exception {
		return stateStore.get().readLog(context.getTenant(), id);
	}

	public void shutdown() {
		scheduler.stop();
		system.shutdown();
		system.awaitTermination();
	}
}
