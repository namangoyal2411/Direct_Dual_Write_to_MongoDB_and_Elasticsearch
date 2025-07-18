package com.Packages.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class ElasticConfiguration {
    @Bean("entityClient")
    @Primary
    public ElasticsearchClient entityClient(
            @Value("${es.entity.host:localhost}") String host,
            @Value("${es.entity.port:9200}") int port
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

    @Bean("metadataClient")
    public ElasticsearchClient metadataClient(
            @Value("${es.metadata.host:localhost}") String host,
            @Value("${es.metadata.port:9201}") int port
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
