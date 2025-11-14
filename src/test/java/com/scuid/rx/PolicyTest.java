package com.scuid.rx;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Module;
import com.alchemain.rx.delegates.NoOp;
import com.alchemain.rx.init.GuiceFactory;
import com.alchemain.rx.init.MongoModule;
import com.alchemain.rx.init.ReactorModule;
import com.alchemain.rx.internal.ReactorCore;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.bus.JsonProvider;
import com.alchemain.rx.bus.JsonFiles;

public class PolicyTest {
	@Test
	public void testNewPerson() throws Exception {
		Module[] m = new Module[] { new ReactorModule(), new MongoModule() };
		GuiceFactory.initialize(m);

		ReactorCore core = GuiceFactory.injector().getInstance(ReactorCore.class);

		ExecutionContext ctx = new ExecutionContext("7598894a-91be-486b-8acb-99b0dad0dcb8");

		JsonNode person = JsonFiles.readTree("tony-stark.json");

		ObjectNode data = JsonProvider.INSTANCE.getMapper().createObjectNode();
		data.put("type", "CREATE");
		data.put("entity", person);

		String id = core.ask(ctx, NoOp.class, data);
		System.out.println("job id: " + id);

		Thread.sleep(1000 * 5);
		core.shutdown();
	}
}
