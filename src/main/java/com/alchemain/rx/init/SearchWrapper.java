package com.alchemain.rx.init;

import java.util.Iterator;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;
import com.alchemain.rx.messages.ExecutionContext;
import com.alchemain.rx.utils.Constants;
import com.alchemain.rx.bus.JsonProvider;

public class SearchWrapper implements Constants {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private Client searchClient;

    @Inject
    public SearchWrapper(Client searchClient) {
        this.searchClient = searchClient;
    }

    public String indexObject(ExecutionContext context, String resource, String _id, JsonNode data) throws Exception {

        // The ES Java API doesn't take a JSON node directly, so we have to
        // stringify it.
        String dataAsString = JsonProvider.INSTANCE.getMapper().writeValueAsString(data);

        IndexResponse response = searchClient.prepareIndex(context.getTenant(), _id).setSource(dataAsString, XContentType.JSON)

                        .execute().actionGet();
        return response.getId();
    }

    public Boolean deleteObject(ExecutionContext context, String resource, String _id) throws Exception {

        DeleteResponse response = searchClient.prepareDelete(context.getTenant(), "_doc", _id).execute().actionGet();
        return response.getResult() == DocWriteResponse.Result.DELETED;
    }

    public JsonNode executeStringQuery(ExecutionContext context, JsonNode requestData) throws Exception {

        SearchResponse response = null;

        int offset = requestData.path(PAGING).get(OFFSET).asInt();
        int limit = requestData.path(PAGING).get(LIMIT).asInt();
        String query = requestData.path(DATA).get(QUERY).asText();

        String resource = null;
        if (requestData.path(DATA).has(RESOURCE)) {
            resource = requestData.path(DATA).get(RESOURCE).asText();
        }

        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery(query);

        if (resource != null) {
            log.debug("Executing search query [{}] on tenant [{}] using type [{}]", query, context.getTenant(),
                    resource);
            response = searchClient.prepareSearch(context.getTenant())
                                .setQuery(queryBuilder).addSort(DISPLAY_NAME, SortOrder.ASC)
                                .setFrom(offset).setSize(limit).execute().actionGet();
        } else {
            log.debug("Executing search query [{}] on tenant [{}] with no specified type", query,
                    context.getTenant());
            response = searchClient.prepareSearch(context.getTenant()).setQuery(queryBuilder)
                    .addSort(DISPLAY_NAME, SortOrder.ASC).setFrom(offset).setSize(limit).execute().actionGet();
        }

        return generatePagedResponse(response, offset, limit);

    }

    /*
     * This query is essentially used as an exact match, and the expectation is
     * that it will return a single result. If a "fuzzier" search is desired
     * (when there could possibly be multiple matches to a single search term),
     * use the executeSimpleQuery method above.
     */
    public JsonNode executeMatchQuery(ExecutionContext context, String resource, String field, String query)
            throws Exception {

        SearchResponse response = null;
        if (resource != null) {
            response = searchClient.prepareSearch(context.getTenant())
                                .setQuery(QueryBuilders.matchQuery(field, query)).execute().actionGet();
        } else {
            response = searchClient.prepareSearch(context.getTenant()).setQuery(QueryBuilders.matchQuery(field, query))
                    .execute().actionGet();
        }

        if (response.getHits().getTotalHits().value != 1) {
            throw new Exception(String.format("Multiple matches found for an exact match query! field: %s, query: %s",
                    field, query));
        }

        Iterator<SearchHit> hit_it = response.getHits().iterator();
        if (hit_it.hasNext()) {
            SearchHit hit = hit_it.next();
            return JsonProvider.INSTANCE.getMapper().readTree(hit.getSourceAsString());
        } else {
            return JsonProvider.INSTANCE.getMapper().createObjectNode();
        }
    }

    private ObjectNode generatePagedResponse(SearchResponse matches, int offset, int limit) throws Exception {

        ObjectNode pagedResponse = JsonProvider.INSTANCE.getMapper().createObjectNode();
        ObjectNode pagingInfo = pagedResponse.putObject(PAGING);
        pagingInfo.put(OFFSET, offset);
        pagingInfo.put(LIMIT, limit);
        pagingInfo.put(TOTAL_COUNT, matches.getHits().getTotalHits().value);
        ArrayNode responseData = pagedResponse.putArray(DATA);

        Iterator<SearchHit> hit_it = matches.getHits().iterator();
        while (hit_it.hasNext()) {
            SearchHit hit = hit_it.next();
            responseData.add(JsonProvider.INSTANCE.getMapper().readTree(hit.getSourceAsString()));
        }

        pagingInfo.put(COUNT, responseData.size());

        return pagedResponse;
    }
}
