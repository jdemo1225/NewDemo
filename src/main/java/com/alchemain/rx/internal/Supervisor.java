package com.alchemain.rx.internal;

import it.sauronsoftware.cron4j.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinPool;

import com.alchemain.rx.internal.Bookie.BookieCreator;
import com.alchemain.rx.messages.Type;
import com.alchemain.rx.messages.Work;
import com.alchemain.rx.scheduler.ScheduledTask;
import com.alchemain.rx.utils.Constants;

/**
 * NOTE: DON'T MODIFY ANY CODE HERE.
 * 
 * Supervisor actor.
 * 
 * @author bilal
 * 
 */

final public class Supervisor extends UntypedActor {
	private final Logger log = LoggerFactory.getLogger(Supervisor.class);

	private final ActorRef worker;
	private final ActorRef bookie;
	private final Scheduler scheduler;

	public Supervisor(Scheduler scheduler) {
		this.worker = getContext().actorOf(Props.create(Worker.class).withRouter(new RoundRobinPool(15)), "worker");
		this.bookie = getContext().actorOf(Props.create(new BookieCreator()).withRouter(new RoundRobinPool(15)),
				"bookie");
		this.scheduler = scheduler;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Work) {
			Work work = (Work) message;
			Type type = work.getType();

			if (type.equals(Type.MoreWork)) {
				onMoreWork(work);
			} else if (type.equals(Type.StartWork)) {
				onStartWork(work);
			}
		} else

		if (message instanceof StoredState) {
			onStoredState((StoredState) message);
		} else if (message instanceof ReadState) {
			onReadState((ReadState) message);
		} else if (message instanceof ErrorState) {
			onErrorState((ErrorState) message);
		} else if (message instanceof Deschedule) {
			onDeschedule((Deschedule) message);
		} else
			throw new RuntimeException("WTF! I don't understand: " + message.getClass().getName());

		if (Constants.DEBUG) {
			trace();
		}
	}

	private void onDeschedule(Deschedule message) {
		log.info("De-scheduling task id: {}", message.getScheduleId());
		scheduler.deschedule(message.getScheduleId());
	}

	private void onStoredState(StoredState storedState) {
		bookie.tell(new ReadState(storedState.getContext(), storedState.getId()), getSelf());
	}

	private void onReadState(ReadState readState) {
		bookie.tell(readState, getSelf());
	}

	private void onMoreWork(Work moreWork) {
		bookie.tell(moreWork, getSelf());
	}

	private void onStartWork(Work startWork) {
		if (startWork.getSchedule() == null) {
			worker.tell(startWork, getSelf());
		} else {
			ScheduledTask task = new ScheduledTask(getSelf(), worker, startWork);
			startWork.setScheduleId(scheduler.schedule(startWork.getSchedule(), task));
		}
	}

	private void onErrorState(ErrorState errorState) {
		bookie.tell(errorState, getSelf());
	}

	private void trace() {
		log.trace("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
		for (ActorRef child : getContext().getChildren()) {
			log.trace("Child actor: {}", child);
		}

		/* Total amount of free memory available to the JVM */
		log.trace("Free memory (bytes): " + Runtime.getRuntime().freeMemory());

		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();

		/* Maximum amount of memory the JVM will attempt to use */
		log.trace("Maximum memory (bytes): " + (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

		/* Total memory currently available to the JVM */
		log.trace("Total memory available to JVM (bytes): " + Runtime.getRuntime().totalMemory());
		log.trace("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	}
}
