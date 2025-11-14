package com.alchemain.rx.internal.delegates;

import javax.inject.Inject;
import javax.inject.Provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.internal.WorkStateStore;
import com.alchemain.rx.messages.Work;

public class Joiner extends Delegate {
	private final Provider<WorkStateStore> stateStor;

	@Inject
	public Joiner(Provider<WorkStateStore> stateStor) {
		this.stateStor = stateStor;
	}

	@Override
	public void execute() throws Exception {
		Work startWork = getStartWork();
		JsonNode data = startWork.getData();

		String delegateClassName = data.get(DELEGATE).asText();

		ObjectNode joinData = mapper.createObjectNode();
		joinData.put(DELEGATE, delegateClassName);
		joinData.put(DATA, data);

		String stackId = startWork.getData().get(JOIN_ON).asText();

		JsonNode fork = stateStor.get().updateFork(startWork.getContext().getTenant(), stackId, joinData);

		if (fork.path(DATA).path(ITERATIONS).path(REMAINING).asInt() == 0) {
			JsonNode responses = fork.get(DATA).get(ITERATIONS).get(RESULT);

			ArrayNode input = mapper.createArrayNode();
			for (JsonNode response : responses) {
				JsonNode responseData = response.get(DATA);
				input.add(responseData);

				// if one of the branches completed successfully, use the
				// delegate specified, else run NoOp delegate. This will still
				// not end and join the other branches, if there were no
				// successful branch executions. Need a better way to select a
				// delegate for error conditions. Maybe the call to forkWork
				// should take a parameter for a delegate to execute on error,
				// so it can join correctly and execute that delegate.
				if (!responseData.has("error")) {
					delegateClassName = response.get(DELEGATE).asText();
				}
			}

			@SuppressWarnings("unchecked")
			Class<? extends Delegate> delegateClass = (Class<? extends Delegate>) Class.forName(delegateClassName);

			log.debug("Joining responses on delegate: [{}]", delegateClassName);
			more(delegateClass, input);
		}
	}
}
