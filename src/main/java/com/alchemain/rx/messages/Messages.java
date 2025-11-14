package com.alchemain.rx.messages;

import it.sauronsoftware.cron4j.SchedulingPattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.alchemain.rx.delegates.Delegate;

public class Messages {
	public static Work moreWork(Class<? extends Delegate> delegate, JsonNode data) {
		return new Work(Type.MoreWork).setDelegate(delegate).setData(data);
	}

	public static Work moreWork(Work work, Class<? extends Delegate> delegate, JsonNode data) {
		return new Work(Type.MoreWork, work, delegate, data);
	}

	public static Work moreWork(Work work, Class<? extends Delegate> delegate, JsonNode data, SchedulingPattern schedule) {
		return new Work(Type.MoreWork, work, delegate, data).setSchedule(schedule);
	}
}
