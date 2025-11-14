package com.alchemain.rx.internal.delegates;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.delegates.Delegate;

public class Forker extends Delegate {
	@Override
	public void execute() throws Exception {
		JsonNode data = getStartWork().getData();

		String delegateClassName = data.get(DELEGATE).asText();
		@SuppressWarnings("unchecked")
		Class<? extends Delegate> delegateClass = (Class<? extends Delegate>) Class.forName(delegateClassName);

		JsonNode forkData = data.get(DATA);

		getStartWork().getContext().pushForkStack(getStartWork().getId());

		for (JsonNode forkItem : forkData) {
			more(delegateClass, forkItem);
		}
	}
}
