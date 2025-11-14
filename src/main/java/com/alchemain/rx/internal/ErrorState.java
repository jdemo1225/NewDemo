package com.alchemain.rx.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.messages.ExecutionContext;

class ErrorState {
	private final ExecutionContext context;
	private final String processId;
	private final String failedStateId;
	private final JsonNode error;

	public ErrorState(ExecutionContext context, String processId, String failedStateId, JsonNode error) {
		this.context = context;
		this.processId = processId;
		this.failedStateId = failedStateId;
		this.error = error;
	}

	public ExecutionContext getContext() {
		return context;
	}

	public String getProcessId() {
		return processId;
	}

	public String getFailedStateId() {
		return failedStateId;
	}

	public JsonNode getError() {
		return error;
	}

	@Override
	public String toString() {
		return String.format("{processId: %s, failedStateId: %s, message: %s}", processId, failedStateId,
				error.toString());
	}
}
