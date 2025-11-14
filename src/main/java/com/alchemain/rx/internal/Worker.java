package com.alchemain.rx.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.UntypedActor;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.delegates.NoOp;
import com.alchemain.rx.init.GuiceFactory;
import com.alchemain.rx.messages.Work;
import com.alchemain.rx.bus.JsonProvider;

/**
 * NOTE: DON'T MODIFY ANY CODE HERE.
 * 
 * A thin delegate wrapper to initialize and process a message. This actor class
 * is responsible for processing work items.
 * 
 * @author bilal
 * 
 */

class Worker extends UntypedActor {
	private final Logger log = LoggerFactory.getLogger(Worker.class);

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Work) {
			Work startWork = (Work) message;

			// instantiate and inject dependencies through guice
			Delegate delegate = GuiceFactory.injector().getInstance(startWork.getDelegate());

			// initialize with current actor context
			delegate._initialize(getSender(), getSelf(), startWork);

			if (log.isTraceEnabled()) {
				log.trace("[{}] - executing with id: [{}], data: [{}]", delegate.getClass().getSimpleName(),
						startWork.getId(), startWork.getData());
			} else {
				log.debug("[{}] - executing with id: [{}]", delegate.getClass().getSimpleName(), startWork.getId());
			}

			try {
				delegate.execute();
			} catch (Throwable t) {
				log.error("Unexpected error in delegate", Throwables.getRootCause(t));

				ObjectNode data = JsonProvider.INSTANCE.getMapper().createObjectNode();
				ObjectNode error = data.putObject("error");
				error.put("exception", t.getClass().getName());
				error.put("message", t.getLocalizedMessage());

				String processId = startWork.getProcessId();

				if (Strings.isNullOrEmpty(processId))
					processId = startWork.getId();

				getSender().tell(new ErrorState(startWork.getContext(), processId, startWork.getId(), data), getSelf());

				// cancel the schedule if this was a scheduled task
				delegate.deschedule();

				// terminate and join, if this was a branch execution
				delegate._join(NoOp.class, data);
			}
		} else
			throw new RuntimeException("WTF! I don't understand: " + message.getClass().getName());
	}
}
