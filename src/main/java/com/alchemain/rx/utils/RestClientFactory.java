package com.alchemain.rx.utils;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.fluent.Request;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.bus.JsonProvider;

public class RestClientFactory implements Constants {
	private final static Logger log = LoggerFactory.getLogger(RestClientFactory.class);
	private static final ObjectMapper MAPPER = JsonProvider.INSTANCE.getMapper();

	/**
	 * Execute a GET request to Scuid proxy API
	 * 
	 * @param context
	 * @param uri
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse getScuidResource(ExecutionContext context, String uri) {
		try {
			Request request = Request.Get(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(AUTHORIZATION, ScuidResourceUtils.getAuthorization(context.getTenant()));
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a POST request to Scuid proxy API
	 * 
	 * @param context
	 * @param uri
	 * @param requestNode
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse postScuidResource(ExecutionContext context, String uri, ObjectNode requestNode) {
		try {
			Request request = Request.Post(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(AUTHORIZATION, ScuidResourceUtils.getAuthorization(context.getTenant()));
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);

			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a GET request to Graphity API
	 * 
	 * @param context
	 * @param uri
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse getGraphityResource(ExecutionContext context, String uri) {
		try {
			Request request = Request.Get(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			request.addHeader(X_TENANT, context.getTenant());

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

//	/**
//	 * Execute a GET request to Lifecycle API
//	 * 
//	 * @param context
//	 * @param uri
//	 * 
//	 * @return RestResponse
//	 * 
//	 * @throws RuntimeException
//	 *             because an exception at this stage is not recoverable and
//	 *             should be propagated.
//	 */
//	public static RestResponse getLifecycleResource(ExecutionContext context, String uri) {
//		try {
//			Request request = Request.Get(uri);
//			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
//			request.addHeader(AUTHORIZATION, context.getAuthorization());
//			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
//
//			return new RestResponse(request.execute().returnResponse());
//		} catch (Throwable t) {
//			throw Throwables.propagate(t);
//		}
//	}
//
//	/**
//	 * Execute a POST request to Lifecycle API
//	 * 
//	 * @param context
//	 * @param uri
//	 * @param requestNode
//	 * 
//	 * @return RestResponse
//	 * 
//	 * @throws RuntimeException
//	 *             because an exception at this stage is not recoverable and
//	 *             should be propagated.
//	 */
//	public static RestResponse postLifecycleResource(ExecutionContext context, String uri, ObjectNode requestNode) {
//		try {
//			Request request = Request.Post(uri);
//			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
//			request.addHeader(AUTHORIZATION, context.getAuthorization());
//			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
//			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
//			// request.useExpectContinue();
//
//			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));
//
//			return new RestResponse(request.execute().returnResponse());
//		} catch (Throwable t) {
//			throw Throwables.propagate(t);
//		}
//	}
//
//	/**
//	 * Execute a DELETE request to Lifecycle API
//	 * 
//	 * @param context
//	 * @param uri
//	 * 
//	 * @return RestResponse
//	 * 
//	 * @throws RuntimeException
//	 *             because an exception at this stage is not recoverable and
//	 *             should be propagated.
//	 */
//	public static RestResponse deleteLifecycleResource(ExecutionContext context, String uri) {
//		try {
//			Request request = Request.Delete(uri);
//			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
//			request.addHeader(AUTHORIZATION, context.getAuthorization());
//			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
//
//			return new RestResponse(request.execute().returnResponse());
//		} catch (Throwable t) {
//			throw Throwables.propagate(t);
//		}
//	}

	/**
	 * Execute a POST request to Hero API
	 * 
	 * @param context
	 * @param uri
	 * @param requestNode
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse postHeroResource(ExecutionContext context, String uri, JsonNode requestNode) {
		try {
			Request request = Request.Post(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(X_TENANT, context.getTenant());
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			// request.useExpectContinue();
			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	public static RestResponse getConnectorResource(ExecutionContext context, String configLocation, String uri) {
		try {
			Request request = Request.Get(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			request.addHeader(X_TENANT, context.getTenant());

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	public static RestResponse postConnectorResource(ExecutionContext context, String configLocation, String uri,
			JsonNode requestNode) {
		try {
			Request request = Request.Post(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			request.addHeader(X_TENANT, context.getTenant());

			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	public static RestResponse deleteConnectorResource(ExecutionContext context, String configLocation, String uri) {
		try {
			Request request = Request.Delete(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);
			request.addHeader(X_TENANT, context.getTenant());

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a GET request to ElasticSearch API
	 * 
	 * @param context
	 * @param uri
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse getSearchResource(ExecutionContext context, String uri) {
		try {
			Request request = Request.Get(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a POST request to ElasticSearch API
	 * 
	 * @param context
	 * @param uri
	 * @param requestNode
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse putSearchResource(ExecutionContext context, String uri, JsonNode requestNode) {
		try {
			Request request = Request.Put(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);

			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a DELETE request to ElasticSearch API
	 * 
	 * @param context
	 * @param uri
	 * @param requestNode
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse deleteSearchResource(ExecutionContext context, String uri) {
		try {
			Request request = Request.Delete(uri);
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	/**
	 * Execute a POST
	 * 
	 * @param context
	 * @param uri
	 * @param requestNode
	 * 
	 * @return RestResponse
	 * 
	 * @throws RuntimeException
	 *             because an exception at this stage is not recoverable and
	 *             should be propagated.
	 */
	public static RestResponse postResource(ExecutionContext context, String uri, JsonNode config, JsonNode requestNode) {
		try {
			Request request = Request.Post(uri);
			// request.useExpectContinue();
			request.connectTimeout(TIMEOUT).socketTimeout(TIMEOUT);
			request.addHeader(CONTENT_TYPE, MediaType.APPLICATION_JSON);
			request.addHeader(ACCEPT, MediaType.APPLICATION_JSON);

			if (config != null) {
				if (config.path("authType").asText().equals("basic")) {
					String username = config.path("username").asText();
					String password = config.path("password").asText();

					Preconditions.checkState(!Strings.isNullOrEmpty(username), Errors.requiredArg("username"));
					Preconditions.checkState(!Strings.isNullOrEmpty(password), Errors.requiredArg("password"));

					request.addHeader(AUTHORIZATION, ScuidResourceUtils.encode(username, password));
				}
			}

			request.bodyByteArray(MAPPER.writeValueAsBytes(requestNode));

			return new RestResponse(request.execute().returnResponse());
		} catch (Throwable t) {
			throw Throwables.propagate(t);
		}
	}

	public static class RestResponse {
		private HttpResponse response;
		private int statusCode;

		public RestResponse(HttpResponse response) {
			this.response = response;
			this.statusCode = response.getStatusLine().getStatusCode();
		}

		/**
		 * 2XX HTTP status codes indicate success
		 * 
		 * @return boolean
		 */
		public boolean isSuccess() {
			return (statusCode / 100) == 2;
		}

		/**
		 * Returns true only if success and response status code is not 204
		 * 
		 * @return boolean
		 */
		public boolean hasContent() {
			return isSuccess() && statusCode != HttpStatus.SC_NO_CONTENT;
		}

		/**
		 * 3XX HTTP status codes indicate re-direction
		 * 
		 * @return boolean
		 */
		public boolean isRedirection() {
			return (statusCode / 100) == 3;
		}

		/**
		 * 4XX HTTP status codes indicate client error
		 * 
		 * @return boolean
		 */
		public boolean isClientError() {
			return (statusCode / 100) == 4;
		}

		/**
		 * 5XX HTTP status codes indicate server error
		 * 
		 * @return boolean
		 */
		public boolean isServerError() {
			return (statusCode / 100) == 5;
		}

		/**
		 * Get HTTP status
		 * 
		 * @return int
		 */
		public int getStatusCode() {
			return statusCode;
		}

		/**
		 * Get HTTP status phrase
		 * 
		 * @return String
		 */
		public String getStatusPhrase() {
			return response.getStatusLine().getReasonPhrase();
		}

		/**
		 * Must be called in order to consume data from input stream and free up
		 * internal resources.
		 * 
		 * @param message
		 * @return
		 * @throws RestException
		 */
		// FIXME: logic for freeing up HTTP resources needs to be reviewed
		public JsonNode asJsonIfSuccessElse(String message) throws RestException {
			if (!hasContent()) {
				JsonNode json = null;
				try {
					json = MAPPER.readTree(response.getEntity().getContent());
				} catch (IOException e) {
					log.warn("Failed to free up HTTP resources properly");
				}

				throw new RestException(message, response.getStatusLine(), json);
			}

			try {
				HttpEntity entity = response.getEntity();

				return MAPPER.readTree(entity.getContent());
			} catch (Throwable t) {
				throw new RestException(message, t);
			}
		}

		/**
		 * Must be called in order to consume data from input stream and free up
		 * internal resources.
		 * 
		 * @param message
		 * @throws RestException
		 */
		// FIXME: logic for freeing up HTTP resources needs to be reviewed
		public void discardIfSuccessElse(String message) throws RestException {
			if (!isSuccess()) {
				JsonNode json = null;
				try {
					json = MAPPER.readTree(response.getEntity().getContent());
				} catch (IOException e) {
					log.warn("Failed to free up HTTP resources properly");
				}

				throw new RestException(message, response.getStatusLine(), json);
			}

			try {
				EntityUtils.consume(response.getEntity());
			} catch (Throwable t) {
				throw new RestException(message, t);
			}
		}
	}

	public static class RestException extends Exception {
		private StatusLine statusLine;
		private JsonNode response;

		public RestException(String message, Throwable t) {
			super(message, t);
		}

		public RestException(String message, StatusLine statusLine) {
			super(message);
			this.statusLine = statusLine;
		}

		public RestException(String message, StatusLine statusLine, JsonNode response) {
			super(message);
			this.statusLine = statusLine;
			this.response = response;
		}

		public StatusLine getStatusLine() {
			return statusLine;
		}

		@Override
		public String getMessage() {
			StringBuffer msg = new StringBuffer();
			msg.append(super.getMessage());

			if (statusLine != null) {
				msg.append("[status code: ").append(statusLine.getStatusCode()).append(", status phrase: ");
				msg.append(statusLine.getReasonPhrase());
				if (response != null) {
					msg.append(", message, ").append(response.path("message").asText());
				}
				msg.append("]");
			}
			return msg.toString();
		}
	}
}
