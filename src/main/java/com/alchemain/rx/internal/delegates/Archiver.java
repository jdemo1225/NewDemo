package com.alchemain.rx.internal.delegates;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.internal.MongoArchive;

public class Archiver extends Delegate {
	private final Logger log = LoggerFactory.getLogger(Archiver.class);
	private MongoArchive archive;
	private final static Pattern SELF_PATTERN = Pattern.compile("/(.*)/.*");

	@Inject
	public Archiver(MongoArchive archive) {
		this.archive = archive;
	}

	@Override
	public void execute() throws Exception {
		JsonNode data = getStartWork().getData();
		log.debug("Received Graphity event for archiving: {}", data);

		if (data.path("type").asText().equals("CREATE")) {
			create();
		} else if (data.path("type").asText().equals("UPDATE")) {
			update();
		} else if (data.path("type").asText().equals("DELETE")) {
			delete();
		}
	}

	private void create() throws Exception {
		JsonNode data = getStartWork().getData();

		JsonNode entity = data.get("entity");

		archive.create(getStartWork().getContext(), parse(), entity);
	}

	private void update() throws Exception {
		JsonNode data = getStartWork().getData();

		JsonNode entity = data.get("entity");

		archive.append(getStartWork().getContext(), parse(), entity);
	}

	private void delete() throws Exception {
		JsonNode data = getStartWork().getData();

		JsonNode entity = data.get("entity");
		archive.append(getStartWork().getContext(), parse(), entity);
	}

	private String parse() {
		String self = getStartWork().getData().path("entity").path("self").asText();
		Matcher matcher = SELF_PATTERN.matcher(self);

		Preconditions.checkState(matcher.matches(), "Malformed Graphity event, doesn't contain a valid self");

		return matcher.group(1);
	}
}
