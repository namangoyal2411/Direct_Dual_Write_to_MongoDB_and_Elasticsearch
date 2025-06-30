package com.packages.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ElasticConfiguration {
    @Bean("entityClient")
    @Primary
    public ElasticsearchClient entityClient(
            @Value("${es.entity.host}")               String host,
            @Value("${es.entity.port}")               int    port,
            @Value("${es.entity.connect-timeout-ms}") int    connectMs,
            @Value("${es.entity.socket-timeout-ms}")  int    socketMs) {

        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
                .setDefaultHeaders(new Header[] {
                        new BasicHeader("Connection", "close")
                })
                .setHttpClientConfigCallback(hc -> hc
                        .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                )
                .setRequestConfigCallback(cfg -> cfg
                        .setConnectTimeout(connectMs)
                        .setSocketTimeout(socketMs)
                );

        RestClient restClient = builder.build();
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());
        JacksonJsonpMapper jacksonMapper = new JacksonJsonpMapper(objectMapper);
        RestClientTransport transport = new RestClientTransport(restClient, jacksonMapper);
        return new ElasticsearchClient(transport);
    }
    @Bean("metadataClient")
    public ElasticsearchClient metadataClient(
            @Value("${es.metadata.host}") String host,
            @Value("${es.metadata.port}") int port
    ) {
        RestClient restClient = RestClient.builder(
                new HttpHost(host, port, "http")
        ).build();

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule());
        JacksonJsonpMapper jacksonMapper = new JacksonJsonpMapper(objectMapper);

        RestClientTransport transport = new RestClientTransport(
                restClient, jacksonMapper
        );

        return new ElasticsearchClient(transport);
    }
}
