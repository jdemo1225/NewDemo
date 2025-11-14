package com.alchemain.rx.rest;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.alchemain.rx.bus.Listener;
import com.alchemain.rx.bus.ListenerCache;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.bus.JsonProvider;

@Path("bus/listeners")
@Singleton
public class ListenersService {
	private static final ObjectMapper MAPPER = JsonProvider.INSTANCE.getMapper();

	private ListenerCache cache;

	@Inject
	public ListenersService(ListenerCache cache) {
		this.cache = cache;
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response readAll(@HeaderParam(Constants.X_TENANT) String tenant) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		ArrayNode response = MAPPER.createObjectNode().putArray("data");

		// Get tenant listeners
		Iterable<Listener> tenantCache = cache.getTenantCache(context);
		for (Listener listener : tenantCache) {
			response.add(MAPPER.convertValue(listener, JsonNode.class));
		}

		return Response.status(Status.OK).entity(response).build();
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response create(@HeaderParam(Constants.X_TENANT) String tenant, JsonNode listener) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		cache.put(context, listener);

		ObjectNode response = MAPPER.createObjectNode();
		response.put(Constants.ID, listener.get(Constants._ID));
		return Response.status(Status.CREATED).entity(response).build();
	}

	@GET
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response read(@HeaderParam(Constants.X_TENANT) String tenant, @PathParam(Constants.ID) String id)
			throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		Listener listener = cache.get(context, id);

		return Response.ok().entity(MAPPER.convertValue(listener, JsonNode.class)).build();
	}

	@PUT
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response update(@HeaderParam(Constants.X_TENANT) String tenant, @PathParam(Constants.ID) String id,
			JsonNode listener) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		((ObjectNode) listener).put(Constants._ID, id);

		cache.put(context, listener);

		ObjectNode response = MAPPER.createObjectNode();
		response.put(Constants.ID, listener.get(Constants._ID));
		return Response.status(Status.OK).entity(response).build();
	}

	@DELETE
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response delete(@HeaderParam(Constants.X_TENANT) String tenant, @PathParam(Constants.ID) String id)
			throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		cache.remove(context, id);
		return Response.noContent().build();
	}

	@POST
	@Path("/resolutions")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response resolve(@HeaderParam(Constants.X_TENANT) String tenant, JsonNode event) throws Exception {

		if (Strings.isNullOrEmpty(tenant))
			return Response.status(Status.BAD_REQUEST).entity(Errors.MISSING_HEADERS).build();

		ExecutionContext context = new ExecutionContext(tenant);

		ObjectNode response = MAPPER.createObjectNode();
		ArrayNode resolutions = response.putArray("resolutions");

		// Check global listeners
		Iterable<Listener> globalCache = cache.getGlobalCache();
		for (Listener listener : globalCache) {
			if (listener.matches(event)) {
				resolutions.add(MAPPER.convertValue(listener, JsonNode.class));
			}
		}

		// Check tenant listeners
		if (!Constants.GLOBAL.equals(context.getTenant())) {
			Iterable<Listener> tenantCache = cache.getTenantCache(context);
			for (Listener listener : tenantCache) {
				if (listener.matches(event)) {
					resolutions.add(MAPPER.convertValue(listener, JsonNode.class));
				}
			}
		}

		return Response.status(Status.OK).entity(response).build();
	}
}
