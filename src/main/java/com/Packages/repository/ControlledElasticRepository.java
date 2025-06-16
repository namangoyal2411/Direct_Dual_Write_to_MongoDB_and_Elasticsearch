package com.Packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
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
    private final double failureRate   = 0.2;
    private final Random random = new Random();
    public ControlledElasticRepository(ElasticsearchClient es) {
        super(es);
    }
    private void simulate(String opName) {
        double r = random.nextDouble();
        if (r < successRate) {
            return;
        }
        double frac = random.nextDouble();
        if (frac <0.2) {
            ErrorResponse err = ErrorResponse.of(b -> b
                    .status(400)
                    .error(e -> e
                            .reason("Simulated invalid‐data failure on " + opName)
                    ));
            throw new ElasticsearchException(opName, err);
        } else {
            throw new RuntimeException("Simulated ES-down failure on " + opName);
        }
    }
    @Override
    public Entity createEntity(String indexName, Entity entity) {
        simulate("create");
        return super.createEntity(indexName, entity);
    }

    @Override
    public Entity updateEntity(String indexName, String id, Entity entity, LocalDateTime ct) {
        simulate("update");
        return super.updateEntity(indexName, id, entity, ct);
    }
    @Override
    public boolean deleteEntity(String indexName, String id) {
        simulate("delete");
        return super.deleteEntity(indexName, id);
    }
}

