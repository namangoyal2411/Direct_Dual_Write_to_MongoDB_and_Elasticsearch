package com.Packages.DLQ;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.Packages.Model.EntityEvent;
import org.springframework.stereotype.Repository;

@Repository
public class DLQElasticRepository {

    private final ElasticsearchClient elasticsearchClient;

    public DLQElasticRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public void saveDLQEvent(String index, EntityEvent event) {
        try {
            IndexResponse response = elasticsearchClient.index(i -> i
                    .index(index)
                    .id(event.getId() + "-" + System.currentTimeMillis()) // avoid ID conflicts
                    .document(event)
            );
            System.out.println("DLQ Event stored: " + response.id());
        } catch (Exception e) {
            System.err.println("Failed to save DLQ event to Elasticsearch: " + e.getMessage());
        }
    }
}

