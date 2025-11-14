package com.alchemain.rx.messages;

import it.sauronsoftware.cron4j.SchedulingPattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.bus.JsonProvider;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Work {
	@JsonIgnore
	private Type type;
	@JsonProperty(value = "_id")
	private String _id;
	private String processId;
	private Class<? extends Delegate> delegate;
	private JsonNode data;
	private ExecutionContext context;

	@JsonSerialize(using = SchedulingPatternSerializer.class)
	@JsonInclude(Include.NON_NULL)
	private SchedulingPattern schedule;
	private String scheduleId;

	public Work() {
	}

	Work(Type type) {
		this.type = type;
	}

	Work(Type type, Work work, Class<? extends Delegate> delegate, JsonNode data) {
		this.type = type;
		this._id = work.getId();
		this.processId = work.getProcessId();
		this.context = work.getContext();
		this.delegate = delegate;
		this.data = data;
	}

	public Type getType() {
		return type;
	}

	public Work setType(Type type) {
		this.type = type;
		return this;
	}

	@JsonProperty(value = "_id")
	public String getId() {
		return _id;
	}

	@JsonProperty(value = "_id")
	public Work setId(String id) {
		this._id = id;
		return this;
	}

	public String getProcessId() {
		return processId;
	}

	public Work setProcessId(String processId) {
		this.processId = processId;
		return this;
	}

	public Class<? extends Delegate> getDelegate() {
		return delegate;
	}

	public Work setDelegate(Class<? extends Delegate> delegate) {
		this.delegate = delegate;
		return this;
	}

	public JsonNode getData() {
		return data;
	}

	public Work setData(JsonNode data) {
		this.data = data;
		return this;
	}

	public ExecutionContext getContext() {
		return context;
	}

	public Work setContext(ExecutionContext context) {
		this.context = context;
		return this;
	}

	public SchedulingPattern getSchedule() {
		return schedule;
	}

	public Work setSchedule(SchedulingPattern schedule) {
		this.schedule = schedule;
		return this;
	}

	public String getScheduleId() {
		return scheduleId;
	}

	public Work setScheduleId(String scheduleId) {
		this.scheduleId = scheduleId;
		return this;
	}

	/**
	 * This convenience method is useful for creating a new data object from
	 * current delegate data. Specified fields are REQUIRED and are copied to
	 * the new object.
	 * 
	 * @param fields
	 * @return newly constructed ObjectNode object
	 */
	public ObjectNode newDataWith(String... fields) {
		ObjectNode newData = JsonProvider.INSTANCE.getMapper().createObjectNode();

		for (String field : fields) {
			JsonNode value = data.path(field);

			Preconditions.checkState(!value.isMissingNode(), Errors.requiredDataAttr(field));

			newData.put(field, value);
		}

		return newData;
	}

	/**
	 * This convenience method is useful for performing precondition checks for
	 * required fields in the delegate data.
	 * 
	 * @param fields
	 */
	public void requireAsText(String... fields) {
		for (String field : fields) {
			JsonNode value = data.path(field);
			Preconditions.checkState(!value.isMissingNode(), Errors.requiredDataAttr(field));
			Preconditions.checkState(!Strings.isNullOrEmpty(value.asText()), Errors.requiredDataAttr(field));
		}
	}

	/**
	 * This convenience method can be used to add a JsonNode object to the
	 * delegate data object. This will replace any existing key/value pairs, if
	 * there is a clash.
	 * 
	 * @param value
	 * @return delegate data object for chaining
	 */
	public JsonNode addToData(JsonNode value) {
		Preconditions.checkArgument(value != null && !value.isMissingNode(), Errors.requiredArg("value"));

		return ((ObjectNode) data).putAll((ObjectNode) value);
	}

	/**
	 * This convenience method can be used to add a specified key/value pair to
	 * delegate data. This will replace any existing key/value pairs, if there
	 * is a clash.
	 * 
	 * @param value
	 * @return delegate data object for chaining
	 */
	public JsonNode addToData(String key, Object value) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(key), Errors.requiredArg("key"));
		Preconditions.checkArgument(value != null, Errors.requiredArg("value"));

		return ((ObjectNode) data).putPOJO(key, value);
	}

	/**
	 * This convenience method can be used to add a specified key/value pair to
	 * delegate data. This will replace any existing key/value pairs, if there
	 * is a clash.
	 * 
	 * @param value
	 * @return delegate data object for chaining
	 */
	public JsonNode addToData(String key, JsonNode value) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(key), Errors.requiredArg("key"));
		Preconditions.checkArgument(value != null && !value.isMissingNode(), Errors.requiredArg("value"));

		((ObjectNode) data).put(key, value);

		return data;
	}

	@Override
	public String toString() {
		return data.toString();
	}
}
