package com.scuid.rx.fault;

public class Progress {
	public final double percent;

	public Progress(double percent) {
		this.percent = percent;
	}

	public String toString() {
		return String.format("%s(%s)", WorkerApi.class.getSimpleName(), percent);
	}
}
