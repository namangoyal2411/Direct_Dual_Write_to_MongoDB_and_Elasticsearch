//package com.packages.repository;
//
//import co.elastic.clients.elasticsearch.ElasticsearchClient;
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import co.elastic.clients.elasticsearch._types.ErrorResponse;
//import com.packages.model.Entity;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Primary;
//import org.springframework.context.annotation.Profile;
//import org.springframework.stereotype.Repository;
//
//import java.io.IOException;
//import java.time.LocalDateTime;
//import java.util.Random;
//
//@Repository
//@Primary
//@Profile("test")
//public class ControlledElasticRepository extends EntityElasticRepository {
//    private final double successRate =0.9;
//    private final double failureRate   = 0.2;
//    private final Random random = new Random();
//    private static final Logger log =
//            LoggerFactory.getLogger(ControlledElasticRepository.class);
//    public ControlledElasticRepository(ElasticsearchClient es) {
//        super(es);
//        log.info(">>>  ControlledElasticRepository ACTIVE");
//    }
//    private void simulate(String opName) {
//        double r = random.nextDouble();
//        if (r < successRate) {
//            return;
//        }
//        double frac = random.nextDouble();
//        if (frac <failureRate) {
//            ErrorResponse err = ErrorResponse.of(b -> b
//                    .status(400)
//                    .error(e -> e
//                            .reason("Simulated invalid‐data failure on " + opName)
//                    ));
//            throw new ElasticsearchException(opName, err);
//        } else {
//            throw new RuntimeException("Simulated ES-down failure on " + opName);
//        }
//    }
//    @Override
//    public Entity createEntity(String indexName, Entity entity) {
//        simulate("create");
//        return super.createEntity(indexName, entity);
//    }
//
//    @Override
//    public Entity updateEntity(String indexName, String id, Entity entity) {
//        simulate("update");
//        return super.updateEntity(indexName, id, entity);
//    }
//    @Override
//    public boolean deleteEntity(String indexName, String id) {
//        simulate("delete");
//        return super.deleteEntity(indexName, id);
//    }
//
//
//}
//
