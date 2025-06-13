package com.Packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.Packages.model.Entity;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Random;

@Repository
@Primary
@Profile("test")
public class ControlledElasticRepository extends EntityElasticRepository {
    private final double successRate;
    private final Random random = new Random();

    public ControlledElasticRepository(ElasticsearchClient elasticsearchClient) {
        super(elasticsearchClient);
        this.successRate = 0.8;
        System.out.println("Using ControlledElasticRepository with successRate = " + successRate);
    }

    private boolean shouldFail() {
        return random.nextDouble() > successRate;
    }

    private void validate(Entity entity) {
        if (entity.getId() == null || entity.getName() == null || entity.getName().isEmpty()) {
            throw new IllegalArgumentException("Invalid entity data");
        }
    }

    @Override
    public Entity createEntity(String indexName, Entity entity) {
        validate(entity);
        if (shouldFail()) {
            throw new RuntimeException("Simulated ES failure on create");
        }
        return super.createEntity(indexName, entity);
    }

    @Override
    public Entity updateEntity(String indexName, String id, Entity entity, LocalDateTime createTime) {
        validate(entity);
        if (shouldFail()) {
            throw new RuntimeException("Simulated ES failure on update");
        }
        return super.updateEntity(indexName, id, entity, createTime);
    }

    @Override
    public boolean deleteEntity(String indexName, String id) {
        if (shouldFail()) {
            throw new RuntimeException("Simulated ES failure on delete");
        }
        return super.deleteEntity(indexName, id);
    }
}
