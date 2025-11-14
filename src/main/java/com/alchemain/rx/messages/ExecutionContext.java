package com.alchemain.rx.messages;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.alchemain.rx.utils.Errors;

public class ExecutionContext {
	private String tenant;
	private List<String> forkStack = new ArrayList<String>(3);
	private List<String> embedStack = new ArrayList<String>(3);

	public ExecutionContext() {
	}

	public ExecutionContext(String tenant) {
		this.tenant = tenant;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public List<String> getForkStack() {
		return forkStack;
	}

	public void setForkStack(List<String> stack) {
		this.forkStack = stack;
	}

	public void pushForkStack(String id) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(id), Errors.requiredArg("id"));

		forkStack.add(id);
	}

	public String popForkStack() {
		if (forkStack.size() == 0)
			return null;

		return forkStack.remove(forkStack.size() - 1);
	}

	public List<String> getEmbedStack() {
		return embedStack;
	}

	public void setEmbedStack(List<String> embedStack) {
		this.embedStack = embedStack;
	}

	public void pushEmbedStack(String id) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(id), Errors.requiredArg("id"));

		embedStack.add(id);
	}

	public String popEmbedStack() {
		if (embedStack.size() == 0)
			return null;

		return embedStack.remove(embedStack.size() - 1);
	}
}
