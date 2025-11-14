package com.alchemain.rx.bus;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public enum JsonProvider {
	INSTANCE() {
		@Override
		public ObjectMapper getMapper() {
			ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
			mapper.disable(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS);
			mapper.setSerializationInclusion(Include.NON_NULL);
			return mapper;
		}
	};

	public abstract ObjectMapper getMapper();
}
