package com.alchemain.rx.internal;

import javax.inject.Provider;

import akka.actor.UntypedActor;
import akka.japi.Creator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.alchemain.rx.init.GuiceFactory;
import com.alchemain.rx.messages.Type;
import com.alchemain.rx.messages.Work;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.bus.JsonProvider;

/**
 * NOTE: DON'T MODIFY ANY CODE HERE.
 * 
 * This actor class is responsible for processing work item state messages.
 * 
 * @author bilal
 * 
 */

class Bookie extends UntypedActor {
	private static final ObjectMapper MAPPER = JsonProvider.INSTANCE.getMapper();

	private Provider<WorkStateStore> stateStore;

	@Inject
	public Bookie(Provider<WorkStateStore> stateStore) {
		this.stateStore = stateStore;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Work) {
			onMoreWork((Work) message);
		} else if (message instanceof ReadState) {
			onReadState((ReadState) message);
		} else if (message instanceof ErrorState) {
			onErrorState((ErrorState) message);
		} else
			throw new RuntimeException("WTF! I don't understand: " + message.getClass().getName());
	}

	private void onMoreWork(Work moreWork) throws Exception {
		String stateId = save(moreWork);
		getSender().tell(new StoredState(moreWork.getContext(), stateId, moreWork.getData()), getSelf());
	}

	/**
	 * Read state from database and create messages. This can be used to replay
	 * messages for aborted/failed work.
	 * 
	 * @param readState
	 * @throws Exception
	 */
	private void onReadState(ReadState readState) throws Exception {
		JsonNode state = stateStore.get().readState(readState.getContext().getTenant(), readState.getId());

		JsonNode context = state.path(Constants.CONTEXT);
		Preconditions.checkState(!context.isMissingNode(), "Execution context missing");

		JsonNode tenant = context.path(Constants.TENANT);
		Preconditions.checkState(!tenant.isMissingNode(), "Tenant missing from execution context");

//		JsonNode auth = context.path(Constants.AUTHORIZATION);
//		Preconditions.checkState(!auth.isMissingNode(), "Authorization missing from execution context");
//
//		JsonNode endpoint = context.path(Constants.ENDPOINT);
//		Preconditions.checkState(!endpoint.isMissingNode(), "Endpoint missing from execution context");

		Work work = MAPPER.convertValue(state, Work.class).setType(Type.StartWork);

		if (readState.getData() != null && readState.getData().size() > 0)
			work.addToData(readState.getData());

		getSender().tell(work, getSelf());
	}

	private String onErrorState(ErrorState error) {
		ObjectNode step = MAPPER.createObjectNode();

		step.put(Constants.PROCESS_ID, error.getProcessId());
		step.put(Constants.FAILED_STATE_ID, error.getFailedStateId());
		step.put(Constants.ERROR, error.getError());

		return stateStore.get().storeState(error.getContext().getTenant(), step);
	}

	private String save(Work work) throws Exception {
		Preconditions.checkState(work.getContext() != null, "Execution context missing");
		Preconditions.checkState(work.getDelegate() != null, "Delegate context missing");

		if (Strings.isNullOrEmpty(work.getProcessId()))
			work.setProcessId(work.getId());

		return stateStore.get().storeState(work);
	}

	public static class BookieCreator implements Creator<Bookie> {
		public Bookie create() {
			return GuiceFactory.injector().getInstance(Bookie.class);
		}
	}
}
