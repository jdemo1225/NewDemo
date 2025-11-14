package com.alchemain.rx.delegates;

import it.sauronsoftware.cron4j.SchedulingPattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.alchemain.rx.internal.Deschedule;
import com.alchemain.rx.internal.ReadState;
import com.alchemain.rx.internal.delegates.Forker;
import com.alchemain.rx.internal.delegates.Joiner;
import com.alchemain.rx.messages.Messages;
import com.alchemain.rx.messages.Type;
import com.alchemain.rx.messages.Work;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.utils.PropertiesUtil;
import com.alchemain.rx.bus.JsonProvider;

/**
 * NOTE: DON'T MODIFY ANY CODE HERE.
 * 
 * Delegate classes are not thread-safe and serve to proxy an actor. Each
 * delegate is tied to actor's lifecycle for processing a message.
 * 
 * @author bilal
 * 
 */
public abstract class Delegate implements Constants {
	protected final Logger log = LoggerFactory.getLogger(this.getClass());

	protected final ObjectMapper mapper = JsonProvider.INSTANCE.getMapper();

	private ActorRef sender;
	private ActorRef self;
	private Work startWork;

	/**
	 * internal use only.
	 * 
	 * @param sender
	 * @param self
	 * @param startWork
	 */
	final public void _initialize(ActorRef sender, ActorRef self, Work startWork) {
		this.sender = sender;
		this.self = self;
		this.startWork = startWork;
	}

	public Work getStartWork() {
		return startWork;
	}

	/**
	 * Send a message for more work.
	 */
	final public void more(Class<? extends Delegate> delegate, JsonNode data) {
		deschedule();

		sender.tell(Messages.moreWork(startWork, delegate, data), self);
	}

	/**
	 * Send a message to schedule work.
	 */
	final public void more(Class<? extends Delegate> delegate, JsonNode data, SchedulingPattern schedule) {
		deschedule();

		sender.tell(Messages.moreWork(startWork, delegate, data, schedule), self);
	}

	/**
	 * Send a message for more work without de-scheduling current delegate.
	 */
	final public void more(Class<? extends Delegate> delegate, JsonNode data, boolean deschedule) {
		if (deschedule)
			deschedule();

		sender.tell(Messages.moreWork(startWork, delegate, data), self);
	}

	/**
	 * Send a message for more work, to process in parallel.
	 */
	final public void fork(Class<? extends Delegate> delegate, JsonNode data) {
		deschedule();

		ObjectNode forkData = mapper.createObjectNode();
		forkData.put(DELEGATE, delegate.getName());
		forkData.put(DATA, data);

		ObjectNode iterations = forkData.putObject(ITERATIONS);
		iterations.put(REMAINING, data.size());
		iterations.put(RESULT, mapper.createArrayNode());

		sender.tell(Messages.moreWork(startWork, Forker.class, forkData), self);
	}

	/**
	 * Send a message for more work, to join parallel work.
	 */
	final public void join(Class<? extends Delegate> delegate, JsonNode data) {
		deschedule();

		_join(delegate, data);
	}

	/**
	 * Send a message to embed an embeddable chain. An embeddable chain must
	 * send a continueWork message upon completion.
	 */
	final public void embed(Class<? extends Delegate> delegate, JsonNode data) {
		deschedule();

		startWork.getContext().pushEmbedStack(getStartWork().getId());

		sender.tell(Messages.moreWork(startWork, delegate, data), self);
	}

	/**
	 * Send a message to continue after the completion of an embeddable chain.
	 * An embeddable chain must send a continueWork message upon completion.
	 */
	final public void yield(Class<? extends Delegate> delegate, JsonNode data) {
		deschedule();

		if (startWork.getContext().getEmbedStack().size() > 0) {
			String embedId = startWork.getContext().popEmbedStack();
			sender.tell(new ReadState(getStartWork().getContext(), embedId, data), self);
		}

		sender.tell(Messages.moreWork(startWork, delegate, data), self);
	}

	/**
	 * Don't call this method directly from a delegate. It's meant for internal
	 * use.
	 * 
	 * @param delegate
	 * @param data
	 */
	final public void _join(Class<? extends Delegate> delegate, JsonNode data) {
		if (startWork.getContext().getForkStack().size() == 0) {
			sender.tell(Messages.moreWork(startWork, delegate, data), self);
		} else {
			ObjectNode joinData = mapper.createObjectNode();
			joinData.put(DELEGATE, delegate.getName());
			joinData.put(DATA, data);
			joinData.put(JOIN_ON, startWork.getContext().popForkStack());

			// setting type to StartWork will cause this delegate to be not
			// persisted in mongodb, instead it will be processed immediately.
			Work joiner = Messages.moreWork(startWork, Joiner.class, joinData).setType(Type.StartWork);

			// setting type to MoreWork will cause this delegate to be persisted
			// first and then processed. default behavior is to not persist.
			if (PropertiesUtil.bool("reactor.joins.persist", false))
				joiner.setType(Type.MoreWork);

			sender.tell(joiner, self);
		}
	}

	/**
	 * Explicitly de-schedule a scheduled delegate execution.
	 */
	final public void deschedule() {
		if (!Strings.isNullOrEmpty(startWork.getScheduleId())) {
			sender.tell(new Deschedule(startWork.getScheduleId()), ActorRef.noSender());
		}
	}

	protected ObjectMapper getObjectMapper() {
		return JsonProvider.INSTANCE.getMapper();
	}

	public abstract void execute() throws Exception;
}
