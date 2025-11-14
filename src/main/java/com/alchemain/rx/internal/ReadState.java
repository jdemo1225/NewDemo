package com.alchemain.rx.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.messages.ExecutionContext;

public class ReadState {
	private final ExecutionContext context;
	private final String id;
	private JsonNode data;

	public ReadState(ExecutionContext context, String id) {
		this.context = context;
		this.id = id;
	}

	public ReadState(ExecutionContext context, String id, JsonNode data) {
		this.context = context;
		this.id = id;
		this.data = data;
	}

	public ExecutionContext getContext() {
		return context;
	}

	public String getId() {
		return id;
	}

	public JsonNode getData() {
		return data;
	}

	public void setData(JsonNode data) {
		this.data = data;
	}

	@Override
	public String toString() {
		return id + ", " + data.toString();
	}
}
