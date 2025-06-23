package com.Packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.UpdateResponse;
import com.Packages.model.Entity;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDateTime;

@Repository
public class EntityElasticRepository {
    private final ElasticsearchClient client;

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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }
    public Entity updateEntity(String indexName,
                               String documentId,
                               Entity entity,
                               LocalDateTime createTime) {
        try {
            UpdateRequest<Entity, Entity> req = UpdateRequest.of(u -> u
                    .index(indexName)
                    .id(documentId)
                    .doc(entity)
            );
            UpdateResponse<Entity> resp = client.update(req, Entity.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
    }

    public boolean deleteEntity(String indexName, String documentId) {
        try {
            DeleteResponse resp = client.delete(d -> d
                    .index(indexName)
                    .id(documentId)
            );
            return resp.result().name().equalsIgnoreCase("Deleted");
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    public Entity createEntityWithVersion(String indexName,
                                          String documentId,
                                          Entity entity,
                                          long version) throws IOException {
        IndexRequest<Entity> req = IndexRequest.of(i -> i
                .index(indexName)
                .id(documentId)
                .version(version)
                .versionType(VersionType.External)
                .document(entity)
        );
        IndexResponse resp = client.index(req);
        return entity;
    }
    public Entity updateEntityWithVersion(
            String indexName,
            String documentId,
            Entity entity,
            long version
    ) throws IOException {
        client.index(i -> i
                .index(indexName)
                .id(documentId)
                .version(version)                  // external version
                .versionType(VersionType.External)
                .document(entity)
        );
        return entity;
    }

    public boolean deleteEntityWithVersion(String indexName,
                                           String documentId,
                                           long version) throws IOException {
        DeleteRequest req = DeleteRequest.of(d -> d
                .index(indexName)
                .id(documentId)
                .version(version)
                .versionType(VersionType.External)
        );
        DeleteResponse resp = client.delete(req);
        return resp.result().name().equalsIgnoreCase("Deleted");
    }
}
