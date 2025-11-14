package com.alchemain.rx.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.bus.JsonProvider;

public class Errors {
	/* REST API ERRORS */
	private final static String REST_GET_FAILED = "Failed to GET resource at URI: [%s]";
	private final static String REST_POST_FAILED = "Failed to POST resource at URI: [%s]";
	private final static String REST_PUT_FAILED = "Failed to PUT resource at URI: [%s]";
	private final static String REST_DELETE_FAILED = "Failed to DELETE resource at URI: [%s]";

	/* GENERIC DATA VALIDATION ERRORS */
	private final static String REQUIRED_DATA_ATTR = "Expecting a non-empty value for attribute: [%s] in job data";
	private final static String REQUIRED_ATTR = "Expecting a non-empty value for attribute: [%s]";
	private final static String REQUIRED_ARG = "Expecting a non-empty value for argument: [%s]";

	/* REQUIRED API FIELDS */
	public static final ObjectNode MISSING_HEADERS = JsonProvider.INSTANCE.getMapper().createObjectNode();
	public static final ObjectNode MISSING_DELEGATE = JsonProvider.INSTANCE.getMapper().createObjectNode();
	public static final ObjectNode MISSING_DATA = JsonProvider.INSTANCE.getMapper().createObjectNode();

	static {
		MISSING_HEADERS.put("error", "Missing  required HTTP headers [tenant]");
		MISSING_DELEGATE.put("error", "Missing job delegate");
		MISSING_DATA.put("error", "Missing job data");
	}
	
	/* LOGIC DATA VALIDATION ERRORS */
	private final static String NO_REQUEST_UNITS = "Request [%S] doesn't have any Request Units";

	public static String restGetFailed(String uri) {
		return String.format(REST_GET_FAILED, uri);
	}

	public static String restPostFailed(String uri) {
		return String.format(REST_POST_FAILED, uri);
	}

	public static String restPutFailed(String uri) {
		return String.format(REST_PUT_FAILED, uri);
	}

	public static String restDeleteFailed(String uri) {
		return String.format(REST_DELETE_FAILED, uri);
	}

	public static String requiredDataAttr(String attr) {
		return String.format(REQUIRED_DATA_ATTR, attr);
	}

	public static String requiredAttr(String attr) {
		return String.format(REQUIRED_ATTR, attr);
	}

	public static String requiredArg(String arg) {
		return String.format(REQUIRED_ARG, arg);
	}

	public static String noRequestUnits(String requestId) {
		return String.format(NO_REQUEST_UNITS, requestId);
	}
}
