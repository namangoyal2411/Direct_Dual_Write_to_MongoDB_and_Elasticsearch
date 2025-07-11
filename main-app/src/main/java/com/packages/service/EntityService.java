package com.packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.model.EntityEvent;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.repository.EntityMongoRepository;
import com.packages.util.EntityUtil;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class EntityService {

    private static final String ES_INDEX  = "entity";
    private static final String DLQ_TOPIC = "Dlq";

    private final EntityMongoRepository      mongoRepo;
    private final EntityElasticRepository    esRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;
    private final EntityMetadataService      metadataService;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    public EntityService(EntityMongoRepository mongoRepo,
                         EntityElasticRepository esRepo,
                         KafkaTemplate<String, EntityEvent> kafka,
                         EntityMetadataService metadataService) {

        this.mongoRepo       = mongoRepo;
        this.esRepo          = esRepo;
        this.kafka           = kafka;
        this.metadataService = metadataService;
    }

    public Entity createEntity(Entity ent) {

        LocalDateTime now = LocalDateTime.now();
        Entity toSave = new Entity(null, ent.getName(), now, now, false, null);

        long mongoWriteMs = System.currentTimeMillis();
        Entity saved      = mongoRepo.createEntity(toSave);

        try {
            esRepo.createEntity(ES_INDEX, saved);

            metadataService.createEntityMetadata(
                    saved, "create", "success",
                    System.currentTimeMillis(), mongoWriteMs, null
            );
            return saved;

        } catch (Exception ex) {
            handleFailure("create", saved, mongoWriteMs, ex);
            throw ex;
        }
    }
    public Entity updateEntity(String id, Entity ent) {

        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));

        Entity updated       = mongoRepo.updateEntity( EntityUtil.updateEntity(ent, existing) );
        long   mongoWriteMs  = System.currentTimeMillis();

        try {
            esRepo.updateEntity(ES_INDEX, id, updated);

            metadataService.createEntityMetadata(
                    updated, "update", "success",
                    System.currentTimeMillis(), mongoWriteMs, null
            );
            return updated;

        } catch (Exception ex) {
            handleFailure("update", updated, mongoWriteMs, ex);
            throw ex;
        }
    }
    public boolean deleteEntity(String id) {

        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));

        Entity markedDeleted = mongoRepo.updateEntity( EntityUtil.markDeleted(existing) );
        long   mongoWriteMs  = System.currentTimeMillis();

        try {
            esRepo.updateEntity(ES_INDEX, id, markedDeleted);

            metadataService.createEntityMetadata(
                    markedDeleted, "delete", "success",
                    System.currentTimeMillis(), mongoWriteMs, null
            );
            return true;

        } catch (Exception ex) {
            handleFailure("delete", markedDeleted, mongoWriteMs, ex);
            throw ex;
        }
    }

    private void handleFailure(String op,
                               Entity entity,
                               long mongoWriteMs,
                               Exception ex) {

        boolean isInvalidData = ex instanceof ElasticsearchException ee
                && ee.status() == 400;

        EntityMetadata meta = metadataService.createEntityMetadata(
                entity, op, "failure", null, mongoWriteMs,ex
        );

        if (!isInvalidData) {
            EntityEvent evt = buildEvent(op, entity, meta.getMetaId());
            kafka.send(DLQ_TOPIC, entity.getId(), evt);
        }
    }


    private EntityEvent buildEvent(String op, Entity entity, String metadataId) {
        return EntityEvent.builder()
                .entity(entity)
                .operation(op)
                .metadataId(metadataId)
                .retryCount(0)
                .build();
    }
}

