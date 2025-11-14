package com.alchemain.rx.internal.delegates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.alchemain.rx.delegates.Delegate;
import com.alchemain.rx.init.SearchWrapper;
import com.alchemain.rx.utils.Errors;
import com.alchemain.rx.utils.PropertiesUtil;
import com.alchemain.rx.utils.RestClientFactory;
import com.alchemain.rx.utils.RestClientFactory.RestException;
import com.alchemain.rx.utils.RestClientFactory.RestResponse;

public class SearchIndexer extends Delegate {
	private final Logger log = LoggerFactory.getLogger(SearchIndexer.class);

	private Provider<SearchWrapper> searchClient;
	
	@Inject
	public SearchIndexer(Provider<SearchWrapper> searchClient) {
		this.searchClient = searchClient;
	}
	
	@Override
	public void execute() throws Exception {
		JsonNode data = getStartWork().getData();
		log.debug("Received Graphity event: {}", data);

		if (data.path("type").asText().equals("CREATE")) {
			put(data.get("entity"));
		} else if (data.path("type").asText().equals("UPDATE")) {
			put(fetchFromGraphity(data.get("entity")));
		} else if (data.path("type").asText().equals("DELETE")) {
			delete(data.get("entity"));
		}
	}

	private void put(JsonNode entity) throws Exception {
		log.debug("Posting event in ElasticSearch index");

		addToSearchIndex(entity);
	}

	private void delete(JsonNode entity) throws Exception {
		log.debug("Deleting event from ElasticSearch index");

		removeFromSearchIndex(entity);
	}

	// FIXME: This should use the SCUID internal proxy API for Graphity, instead
	// of calling Graphity directly
	private JsonNode fetchFromGraphity(JsonNode entity) throws RestException {
		StringBuilder uri = new StringBuilder(PropertiesUtil.string("graphity.api.graph.uri"));
		uri = uri.append(entity.get("self").asText());

		RestResponse get = RestClientFactory.getGraphityResource(getStartWork().getContext(), uri.toString());

		return get.asJsonIfSuccessElse(Errors.restGetFailed(uri.toString()));
	}

	private void addToSearchIndex(JsonNode entity) throws Exception {
		String slashless = entity.get("self").asText().substring(1);
		String[] selfInfo = slashless.split("/");
		String response = searchClient.get().indexObject(getStartWork().getContext(), selfInfo[0], selfInfo[1], entity);
		
		if (response == null || !response.equals(selfInfo[1])) {
			log.error("Response from Elasticsearch index request does not match entity data!");
		}
		
//		StringBuilder uri = new StringBuilder(PropertiesUtil.string("search.api.uri"));
//		uri.append("/").append(getStartWork().getContext().getTenant());
//		uri = uri.append(entity.get("self").asText());
//
//		RestResponse put = RestClientFactory.putSearchResource(getStartWork().getContext(), uri.toString(), entity);
//
//		put.discardIfSuccessElse(Errors.restPutFailed(uri.toString()));
	}

	private void removeFromSearchIndex(JsonNode entity) throws Exception {
		String slashless = entity.get("self").asText().substring(1);
		String[] selfInfo = slashless.split("/");
		boolean found = searchClient.get().deleteObject(getStartWork().getContext(), selfInfo[0], selfInfo[1]);
		
		if (!found) {
			log.error("Entity [{}] not found in search index. Delete failed!", 
					entity.get(_ID).asText());
		}
//
//		
//		StringBuilder uri = new StringBuilder(PropertiesUtil.string("search.api.uri"));
//		uri.append("/").append(getStartWork().getContext().getTenant());
//		uri = uri.append(entity.get("self").asText());
//
//		RestResponse delete = RestClientFactory.deleteSearchResource(getStartWork().getContext(), uri.toString());
//
//		delete.discardIfSuccessElse(Errors.restDeleteFailed(uri.toString()));
	}
}
