package com.alchemain.rx.messages;

import it.sauronsoftware.cron4j.SchedulingPattern;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class SchedulingPatternSerializer extends JsonSerializer<SchedulingPattern> {

	@Override
	public void serialize(SchedulingPattern pattern, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jgen.writeString(pattern.toString());
	}
}
