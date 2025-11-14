package com.alchemain.rx.internal;

import it.sauronsoftware.cron4j.Scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.japi.Creator;

public class SupervisorCreator implements Creator<Supervisor> {
	private final static Logger log = LoggerFactory.getLogger(SupervisorCreator.class);

	private final Scheduler scheduler;

	public SupervisorCreator(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	public Supervisor create() {
		log.debug("Creating a new supervisor");
		return new Supervisor(scheduler);
	}
}
