package com.alchemain.rx.bus;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Resources;
import com.alchemain.rx.bus.JsonProvider;

public class JsonFiles {
        public static JsonNode readTree(String name) throws JsonProcessingException, IOException {
                return JsonProvider.INSTANCE.getMapper().readTree(Resources.getResource(name));
        }

        public static <T> T readAs(String file, Class<T> type) throws Exception {
                return JsonProvider.INSTANCE.getMapper().convertValue(readTree(file), type);
        }

        public static <T> T readAs(String file, String object, Class<T> type) throws Exception {
                return JsonProvider.INSTANCE.getMapper().convertValue(readTree(file).get(object), type);
        }
}
