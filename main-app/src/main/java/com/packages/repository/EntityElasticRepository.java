package com.packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;

import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import com.packages.model.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;

@Repository
public class EntityElasticRepository {
    private final ElasticsearchClient client;

    @Autowired
    public EntityElasticRepository(ElasticsearchClient client) {
        this.client = client;
    }

    public Entity createEntity(String indexName, Entity entity) {
        try {
            IndexRequest<Entity> req = IndexRequest.of(i -> i
                    .index(indexName)
                    .id(entity.getId())
                    .document(entity)
            );
            IndexResponse resp = client.index(req);
            return entity;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Entity updateEntity(String indexName,
                               String documentId,
                               Entity entity) {
        try {
            UpdateRequest<Entity, Entity> req = UpdateRequest.of(u -> u
                    .index(indexName)
                    .id(documentId)
                    .doc(entity)
                    .docAsUpsert(true)
            );
            UpdateResponse<Entity> resp = client.update(req, Entity.class);
            return entity;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}