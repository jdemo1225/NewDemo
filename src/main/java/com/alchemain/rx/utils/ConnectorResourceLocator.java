package com.alchemain.rx.utils;

import com.alchemain.rx.messages.ExecutionContext;

public class ConnectorResourceLocator {
	private static final String CF_API_BASE_URI = PropertiesUtil.string("cf.api.uri");

	/* Connector APIs */
	private static final String USERS_URI = "%s/Users";
	private static final String USERS_URI_PATTERN = "%s/Users/%s";
	private static final String POST_USERS_DELETE_URI_PATTERN = "%s/Users/%s/Delete";
	private static final String POST_USERS_DISABLE_URI_PATTERN = "%s/Users/%s/Disable";
	private static final String USERS_PASSWORD_URI_PATTERN = "%s/Users/%s/Password";
	private static final String GROUPS_URI_PATTERN = "%s/Groups/%s";
	private static final String TICKETS_URI_PATTERN = "%s/Tickets/%s";

	/* Connector API URIs */
	public static String getUsersLocation(ExecutionContext context) {
		return String.format(USERS_URI, CF_API_BASE_URI);
	}

	public static String getUsersLocation(ExecutionContext context, String userId) {
		return String.format(USERS_URI_PATTERN, CF_API_BASE_URI, userId);
	}

	public static String getUsersDeleteLocation(ExecutionContext context, String userId) {
		return String.format(POST_USERS_DELETE_URI_PATTERN, CF_API_BASE_URI, userId);
	}

	public static String getUsersDisableLocation(ExecutionContext context, String userId) {
		return String.format(POST_USERS_DISABLE_URI_PATTERN, CF_API_BASE_URI, userId);
	}

	public static String getUsersPasswordLocation(ExecutionContext context, String userId) {
		return String.format(USERS_PASSWORD_URI_PATTERN, CF_API_BASE_URI, userId);
	}

	public static String getGroupsLocation(ExecutionContext context, String groupId) {
		return String.format(GROUPS_URI_PATTERN, CF_API_BASE_URI, groupId);
	}

	public static String getTicketsLocation(ExecutionContext context, String ticketId) {
		return String.format(TICKETS_URI_PATTERN, CF_API_BASE_URI, ticketId);
	}
}
