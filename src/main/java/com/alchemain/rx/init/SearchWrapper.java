package com.alchemain.rx.init;

import java.util.Iterator;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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

    private RestHighLevelClient searchClient;

    @Inject
    public SearchWrapper(RestHighLevelClient searchClient) {
        this.searchClient = searchClient;
    }

    public String indexObject(ExecutionContext context, String resource, String _id, JsonNode data) throws Exception {

        // The ES Java API doesn't take a JSON node directly, so we have to
        // stringify it.
        String dataAsString = JsonProvider.INSTANCE.getMapper().writeValueAsString(data);

        IndexRequest request = new IndexRequest(context.getTenant())
                .index(resource)
                .id(_id)
                .source(dataAsString, XContentType.JSON);
        
        IndexResponse response = searchClient.index(request);
        return response.getId();
    }

    public Boolean deleteObject(ExecutionContext context, String resource, String _id) throws Exception {

        DeleteRequest request = new DeleteRequest(context.getTenant(), resource, _id);
        DeleteResponse response = searchClient.delete(request);
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

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .from(offset)
                .size(limit)
                .sort(DISPLAY_NAME, SortOrder.ASC);

        SearchRequest searchRequest = new SearchRequest(context.getTenant());
        
        if (resource != null) {
            log.debug("Executing search query [{}] on tenant [{}] using type [{}]", query, context.getTenant(),
                    resource);
            searchRequest.types(resource);
            searchRequest.source(sourceBuilder);
            response = searchClient.search(searchRequest);
        } else {
            log.debug("Executing search query [{}] on tenant [{}] with no specified type", query,
                    context.getTenant());
            searchRequest.source(sourceBuilder);
            response = searchClient.search(searchRequest);
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
        
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchQuery(field, query));
        
        SearchRequest searchRequest = new SearchRequest(context.getTenant());
        
        if (resource != null) {
            searchRequest.types(resource);
            searchRequest.source(sourceBuilder);
            response = searchClient.search(searchRequest);
        } else {
            searchRequest.source(sourceBuilder);
            response = searchClient.search(searchRequest);
        }

        if (response.getHits().getTotalHits() != 1) {
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
        pagingInfo.put(TOTAL_COUNT, matches.getHits().getTotalHits());
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
