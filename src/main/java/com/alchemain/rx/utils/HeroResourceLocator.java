package com.alchemain.rx.utils;

import com.alchemain.rx.messages.ExecutionContext;

public class HeroResourceLocator {
	private static final String HERO_API_BASE_URI = PropertiesUtil.string("hero.api.uri");

	/* Hero APIs */
	private static final String MESSAGES = "%s/NotificationService/Messages";
	private static final String LOGS = "%s/LogService/Logs";

	/* Hero API URIs */
	public static String getNotificationsLocation(ExecutionContext context) {
		return String.format(MESSAGES, HERO_API_BASE_URI);
	}

	public static String getLogsLocation(ExecutionContext context) {
		return String.format(LOGS, HERO_API_BASE_URI);
	}
}
