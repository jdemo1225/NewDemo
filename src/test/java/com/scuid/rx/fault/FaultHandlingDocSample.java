package com.scuid.rx.fault;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class FaultHandlingDocSample {

	/**
	 * Runs the sample
	 */
	public static void main(String[] args) {
		Config config = ConfigFactory.parseString("akka.loglevel = DEBUG \n" + "akka.actor.debug.lifecycle = on");

		ActorSystem system = ActorSystem.create("FaultToleranceSample", config);
		ActorRef worker = system.actorOf(Props.create(Worker.class), "worker");
		ActorRef listener = system.actorOf(Props.create(Listener.class), "listener");
		// start the work and listen on progress
		// note that the listener is used as sender of the tell,
		// i.e. it will receive replies from the worker
		worker.tell(WorkerApi.Start, listener);
	}
}
