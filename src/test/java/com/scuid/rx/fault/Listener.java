package com.scuid.rx.fault;

import scala.concurrent.duration.Duration;
import akka.actor.ReceiveTimeout;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Listens on progress from the worker and shuts down the system when enough
 * work has been done.
 */
public class Listener extends UntypedActor {
	final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

	@Override
	public void preStart() {
		// If we don't get any progress within 15 seconds then the service is
		// unavailable
		getContext().setReceiveTimeout(Duration.create("15 seconds"));
	}

	public void onReceive(Object msg) {
		log.debug("received message {}", msg);
		if (msg instanceof Progress) {
			Progress progress = (Progress) msg;
			log.info("Current progress: {} %", progress.percent);
			if (progress.percent >= 100.0) {
				log.info("That's all, shutting down");
				getContext().system().shutdown();
			}
		} else if (msg == ReceiveTimeout.getInstance()) {
			// No progress within 15 seconds, ServiceUnavailable
			log.error("Shutting down due to unavailable service");
			getContext().system().shutdown();
		} else {
			unhandled(msg);
		}
	}
}
