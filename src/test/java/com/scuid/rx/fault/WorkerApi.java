package com.scuid.rx.fault;

public interface WorkerApi {
	public static final Object Start = "Start";
	public static final Object Do = "Do";

	public interface StorageApi {

		public static class Store {
			public final StorageApi.Entry entry;

			public Store(StorageApi.Entry entry) {
				this.entry = entry;
			}

			public String toString() {
				return String.format("%s(%s)", WorkerApi.class.getSimpleName(), entry);
			}
		}

		public static class Entry {
			public final String key;
			public final long value;

			public Entry(String key, long value) {
				this.key = key;
				this.value = value;
			}

			public String toString() {
				return String.format("%s(%s, %s)", WorkerApi.class.getSimpleName(), key, value);
			}
		}

		public static class Get {
			public final String key;

			public Get(String key) {
				this.key = key;
			}

			public String toString() {
				return String.format("%s(%s)", WorkerApi.class.getSimpleName(), key);
			}
		}
	}
}
