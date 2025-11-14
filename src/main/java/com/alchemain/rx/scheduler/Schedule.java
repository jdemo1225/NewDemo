package com.alchemain.rx.scheduler;

import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.SchedulingPattern;

import java.util.Calendar;

import com.google.common.base.Preconditions;

/**
 * Utility class for generating common cron expressions.
 * 
 * @author bilal
 * 
 */
public class Schedule {
	/**
	 * Generate cron-like expression for executing a task once a year at a
	 * specific date and start time, in minutes.
	 * 
	 * NOTE: The year part of the calendar is ignored.
	 * 
	 * @param schedule
	 *            date
	 * @param start
	 *            time in minutes
	 * 
	 * @return cron expression
	 * 
	 */
	public static SchedulingPattern every(Calendar cal, int start) {
		int minutes = cal.get(Calendar.MINUTE);
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

		// +1 because first java month is 0
		int month = cal.get(Calendar.MONTH) + 1;

		String pattern = String.format("%d %d %d %d *", minutes + start, hour, dayOfMonth, month);

		Preconditions.checkState(SchedulingPattern.validate(pattern), "Invalid cron expression generated: " + pattern);

		return new SchedulingPattern(pattern);
	}

	/**
	 * Generate cron-like expression for executing a task repeating every
	 * minute, specified by the minute interval starting now plus interval.
	 * 
	 * @param interval
	 *            in minutes
	 * 
	 * @return cron expression
	 */
	public static SchedulingPattern everyMinute(int interval) {
		int minute = Calendar.getInstance().get(Calendar.MINUTE);

		String pattern = String.format("%d-%d/%d * * * *", minute, minute - 1, interval);

		Preconditions.checkState(SchedulingPattern.validate(pattern), "Invalid cron expression generated: " + pattern);

		return new SchedulingPattern(pattern);
	}

	public static void main(String[] args) {
		dump(every(Calendar.getInstance(), 1));

		dump(everyMinute(2));
	}

	private static void dump(SchedulingPattern pattern) {
		System.out.println(pattern);

		Predictor predictor = new Predictor(pattern);

		for (int i = 0; i < 60; i++) {
			System.out.println(predictor.nextMatchingDate());
		}
	}
}
