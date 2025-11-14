package com.alchemain.rx.scheduler;

import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskExecutionContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;

import com.alchemain.rx.messages.Work;

public class ScheduledTask extends Task {
	private final static Logger log = LoggerFactory.getLogger(ScheduledTask.class);

	private final ActorRef supervisor;
	private final ActorRef worker;
	private final Work work;

	public ScheduledTask(ActorRef supervisor, ActorRef worker, Work work) {
		this.supervisor = supervisor;
		this.worker = worker;
		this.work = work;
	}

	@Override
	public void execute(TaskExecutionContext context) throws RuntimeException {
		log.info("Executing: {}, with data: {}", this.getClass().getName(), work);

		worker.tell(work, supervisor);
	}
}
