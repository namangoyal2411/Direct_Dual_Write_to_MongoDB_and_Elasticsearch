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
    private final double successRate =0.8;
    private final double invalidRate   = 0.2;
    private final Random random = new Random();


    public ControlledElasticRepository(ElasticsearchClient es) {
        super(es);
        System.out.println("Using ControlledElasticRepository with successRate=" + successRate
                + ", invalidRate=" + invalidRate);
    }
    private void simulate(String operation) {
        double r = random.nextDouble();
        if (r < successRate) {
            return;  // success
        }
        double frac = (r - successRate) / (1 - successRate);
        if (frac < invalidRate) {
            throw new IllegalArgumentException("Simulated invalid‐data failure"+operation);
        } else {
            throw new RuntimeException("Simulated ES‐down failure"+operation);
        }
    }

    private void validate(Entity e) {
        if (e.getId() == null || e.getName() == null || e.getName().isBlank()) {
            throw new IllegalArgumentException("Invalid entity data");
        }
    }

    @Override
    public Entity createEntity(String indexName, Entity entity) {
        validate(entity);
        simulate("create");
        return super.createEntity(indexName, entity);
    }

    @Override
    public Entity updateEntity(String indexName, String id, Entity entity, LocalDateTime ct) {
        validate(entity);
        simulate("update");
        return super.updateEntity(indexName, id, entity, ct);
    }

    @Override
    public boolean deleteEntity(String indexName, String id) {
        simulate("delete");
        return super.deleteEntity(indexName, id);
    }
}

