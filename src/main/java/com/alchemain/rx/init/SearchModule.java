package com.alchemain.rx.init;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.alchemain.rx.utils.PropertiesUtil;

public class SearchModule extends AbstractModule {

    private final static Logger log = LoggerFactory.getLogger(SearchModule.class);

    private final String CLIENT_HOSTS = "search.transport.hosts";
    private final String CLIENT_METHOD = "search.client.method";
    private final String CLUSTER_NAME = "search.cluster.name";

    @Override
    protected void configure() {
        bind(RestHighLevelClient.class).toInstance(esConnect());
        bind(SearchWrapper.class).in(Scopes.SINGLETON);
    }

    public RestHighLevelClient esConnect() {
        String transportMethod = PropertiesUtil.string(CLIENT_METHOD);
        String clusterName = PropertiesUtil.string(CLUSTER_NAME);
        if (clusterName == null)
            clusterName = "elasticsearch";

        log.trace("Creating ES Client:  cluster = {}, transport = {}", clusterName, transportMethod);

        String[] hosts = PropertiesUtil.string(CLIENT_HOSTS).split(",");
        List<HttpHost> httpHosts = new ArrayList<>();
        for (String host : hosts) {
            String[] constituents = host.split(":");
            try {
                httpHosts.add(new HttpHost(constituents[0], Integer.parseInt(constituents[1]), "http"));
            } catch (Exception e) {
                log.error("Failed to parse host: {}", e.getMessage());
            }
        }
        
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}

