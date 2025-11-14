package com.alchemain.rx.scheduler;

import it.sauronsoftware.cron4j.SchedulingPattern;
import it.sauronsoftware.cron4j.Task;
import it.sauronsoftware.cron4j.TaskCollector;
import it.sauronsoftware.cron4j.TaskExecutionContext;
import it.sauronsoftware.cron4j.TaskTable;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.alchemain.rx.bus.JsonProvider;

/**
 * 
 * @author bilal
 * 
 */
public class MongoTaskCollector implements TaskCollector {
	private final static Logger log = LoggerFactory.getLogger(MongoTaskCollector.class);

	public static AtomicInteger COUNTER = new AtomicInteger();

	@Override
	public TaskTable getTasks() {
		TaskTable table = new TaskTable();

		ArrayNode tasks = getTasksFromStore();

		for (JsonNode task : tasks) {
			CallbackTask t = new CallbackTask(task.get("data"));
			SchedulingPattern pattern = new SchedulingPattern(task.get("pattern").asText());
			table.add(pattern, t);
		}

		return table;
	}

	private static ArrayNode getTasksFromStore() {
		ArrayNode tasks = JsonProvider.INSTANCE.getMapper().createArrayNode();

		for (int i = 0; i < 50000; i++) {
			ObjectNode task = tasks.addObject();
			task.put("pattern", "* * * * *");
			task.put("task", CallbackTask.class.getName());
			ObjectNode data = task.putObject("data");
			ObjectNode context = data.putObject("context");
			context.put("tenant", "midas");
			context.put("authorization", "");
			context.put("endpoint", "");
			data.put("jobId", "job-" + i);
		}

		return tasks;
	}

	public static class CallbackTask extends Task {
		private JsonNode data;

		public CallbackTask(JsonNode data) {
			this.data = data;
		}

		@Override
		public void execute(TaskExecutionContext context) throws RuntimeException {
			log.info("Executing: {}, with data: {}", this.getClass().getName(), data);

			COUNTER.incrementAndGet();
			// context.getScheduler().deschedule(context.);
		}
	}
}
