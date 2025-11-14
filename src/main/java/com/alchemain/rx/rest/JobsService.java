package com.alchemain.rx.rest;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.alchemain.rx.internal.ReactorCore;
import com.alchemain.rx.internal.delegates.EventRouter;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.bus.JsonProvider;

@Path("jobs")
@Singleton
public class JobsService {
	private static final ObjectMapper MAPPER = JsonProvider.INSTANCE.getMapper();

	private Provider<ReactorCore> reactorCore;

	@Inject
	public JobsService(Provider<ReactorCore> reactorCore) {
		this.reactorCore = reactorCore;
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createJob(@HeaderParam(Constants.X_TENANT) String tenant, JsonNode payload) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		if (Strings.isNullOrEmpty(payload.path(Constants.DELEGATE).asText()))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_DELEGATE).build();

		if (payload.get(Constants.DATA) == null)
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_DATA).build();

		String delegate = payload.get(Constants.DELEGATE).asText();

		String id = reactorCore.get().ask(context, delegate, payload.get(Constants.DATA));
		ObjectNode response = MAPPER.createObjectNode();
		response.put(Constants.ID, id);
		return Response.status(Status.ACCEPTED).entity(response).build();
	}

	@GET
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response readJob(@HeaderParam(Constants.X_TENANT) String tenant, @PathParam(Constants.ID) String id)
			throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		return Response.ok().entity(reactorCore.get().log(context, id)).build();
	}

	@POST
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response restartJob(@HeaderParam(Constants.X_TENANT) String tenant, @PathParam(Constants.ID) String id,
			JsonNode data) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		ObjectNode response = MAPPER.createObjectNode();
		response.put(Constants.ID, id);
		response.put(Constants.STATUS, Constants.PENDING);

		reactorCore.get().restart(context, id, data);

		return Response.status(Status.ACCEPTED).entity(response).build();
	}

	@POST
	@Path("/events")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createEvent(@HeaderParam(Constants.X_TENANT) String tenant, JsonNode payload) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		Preconditions
				.checkArgument(!Strings.isNullOrEmpty(payload.path("topic").asText()), Errors.requiredArg("topic"));

		ExecutionContext context = new ExecutionContext(tenant);

		String id = reactorCore.get().ask(context, EventRouter.class, payload);

		ObjectNode response = MAPPER.createObjectNode();
		response.put(Constants.ID, id);
		return Response.status(Status.ACCEPTED).entity(response).build();
	}
}
