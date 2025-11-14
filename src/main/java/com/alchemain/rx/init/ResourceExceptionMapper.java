package com.alchemain.rx.init;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.alchemain.rx.bus.JsonProvider;

@Provider
public class ResourceExceptionMapper implements ExceptionMapper<Throwable> {
	private static final Logger log = LoggerFactory.getLogger(ResourceExceptionMapper.class);
	private static final ObjectMapper MAPPER = JsonProvider.INSTANCE.getMapper();

	@Override
	public Response toResponse(Throwable t) {
		Throwable root = Throwables.getRootCause(t);
		String message = t.getLocalizedMessage();
		log.error(message, root);

		if (t instanceof IllegalArgumentException)
			return ResourceExceptionMapper.getError(Status.BAD_REQUEST, message, root);
		else
			return ResourceExceptionMapper.getError(Status.INTERNAL_SERVER_ERROR, message, root);
	}

	private static Response getError(Status status, String message, Throwable root) {
		int code = status.getStatusCode();
		if (log.isDebugEnabled()) {
			message = Throwables.getStackTraceAsString(root);
		}
		ObjectNode payload = MAPPER.createObjectNode();
		payload.put("code", code).put("message", message);
		return Response.status(status).entity(payload.toString()).build();
	}
}
