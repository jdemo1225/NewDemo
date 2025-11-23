package com.alchemain.rx.init;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
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
        bind(Client.class).toInstance(esConnect());
        bind(SearchWrapper.class).in(Scopes.SINGLETON);
    }

    public Client esConnect() {
        String transportMethod = PropertiesUtil.string(CLIENT_METHOD);
        String clusterName = PropertiesUtil.string(CLUSTER_NAME);
        if (clusterName == null)
            clusterName = "elasticsearch";

        log.trace("Creating ES Client:  cluster = {}, transport = {}", clusterName, transportMethod);

        String[] hosts = PropertiesUtil.string(CLIENT_HOSTS).split(",");
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        TransportClient client = new PreBuiltTransportClient(settings);
        for (String host : hosts) {
            String[] constituents = host.split(":");
            try {
                client.addTransportAddress(new TransportAddress(InetAddress.getByName(constituents[0]), Integer.parseInt(constituents[1])));
            } catch (UnknownHostException e) {
                log.error("Failed to add transport address: {}", e.getMessage());
            }
        }
        return client;
    }
}

