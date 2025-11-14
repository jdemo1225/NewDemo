package com.scuid.rx.greet;

import akka.actor.UntypedActor;

public class GreetPrinter extends UntypedActor {
	public void onReceive(Object message) {
		if (message instanceof Greeting)
			System.out.println(((Greeting) message).message);
	}
}
