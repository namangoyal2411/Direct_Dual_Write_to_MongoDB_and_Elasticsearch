package com.Packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsResponse;
import com.Packages.model.Entity;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public class EntityElasticRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityElasticRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }
    public Entity createEntity(String indexName, Entity entity) {
        try {
            IndexRequest<Entity> request = IndexRequest.of(i -> i
                    .index(indexName)
                    .id(entity.getId())
                    .document(entity)
            );
            IndexResponse response = elasticsearchClient.index(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }
    public Entity updateEntity(String indexName, String documentId, Entity entity, LocalDateTime createTime) {
        try {
            UpdateRequest<Entity, Entity> request = UpdateRequest.of(u -> u
                    .index(indexName)
                    .id(documentId)
                    .doc(entity)
            );
            UpdateResponse<Entity> response = elasticsearchClient.update(request, Entity.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }
    public boolean deleteEntity(String indexName, String documentId) {
        try {
            elasticsearchClient.delete(d -> d.index(indexName).id(documentId));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}