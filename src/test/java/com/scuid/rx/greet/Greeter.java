package com.scuid.rx.greet;

import akka.actor.UntypedActor;

public class Greeter extends UntypedActor {
	String greeting = "";

	public void onReceive(Object message) {
		if (message instanceof WhoToGreet)
			greeting = "hello, " + ((WhoToGreet) message).who;

		else if (message instanceof Greet)
			// Send the current greeting back to the sender
			getSender().tell(new Greeting(greeting), getSelf());

		else
			unhandled(message);
	}
}
