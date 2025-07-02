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
import java.util.UUID;

@Service
public class EntityService {
    private static final String ES_INDEX = "entity";
    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;
    private final EntityMetadataService entityMetadataService;
    public EntityService(EntityMongoRepository mongoRepo,
                         EntityElasticRepository esRepo,
                         KafkaTemplate<String, EntityEvent> kafka,EntityMetadataService entityMetadataService) {
        this.mongoRepo = mongoRepo;
        this.esRepo = esRepo;
        this.kafka = kafka;
        this.entityMetadataService = entityMetadataService;
    }
    String dlqTopic ="dlq136";
    public Entity createEntity(Entity ent) {
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = new Entity(null, ent.getName(), now, now,false, null);
        long mongoWriteMillis = System.currentTimeMillis();
        Entity saved = mongoRepo.createEntity(toSave);
        try {
            long esWriteMillis = System.currentTimeMillis();
            esRepo.createEntity(ES_INDEX, saved);
            entityMetadataService.createEntityMetadata(
                    saved,
                    "create",
                    "success",
                    esWriteMillis,
                    mongoWriteMillis,
                    null
            );
            return saved;
        }  catch (Exception ex) {
            boolean isInvalidData = false;
            Throwable cause = ex;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            String rootClass = cause.getClass().getSimpleName();
            String msg       = cause.getMessage() == null
                    ? ""
                    : cause.getMessage().toLowerCase();

            // 2) bucket by root exception type or message
            String reason;
            if ("ResponseException".equals(rootClass)
                    || msg.contains("429")
                    || msg.contains("too many requests")) {
                reason = "HTTP429";
            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
                    || msg.contains("connect timed out")) {
                reason = "ConnectTimeout";
            } else if ("SocketTimeoutException".equals(rootClass)
                    || msg.contains("timeout on connection")
                    || msg.contains("read timeout")) {
                reason = "ReadTimeout";
            } else {
                // any other root cause (e.g. some other IO error)
                reason = rootClass;
            }
            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
                isInvalidData = true;
            }
           EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
                    saved, "create", "failure",
                    null, mongoWriteMillis, reason
            );
            if (!isInvalidData) {
                EntityEvent entityEvent = buildEvent("create",saved,entityMetadata.getMetaId());
                kafka.send(dlqTopic, saved.getId(), entityEvent);
            }
        throw ex ;
        }

    }

    public Entity updateEntity(String id, Entity entity) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity updated = EntityUtil.updateEntity(entity, existing);
        long mongoWriteMillis = System.currentTimeMillis();
        updated = mongoRepo.updateEntity(updated);
        try {
            long esWriteMillis = System.currentTimeMillis();
            esRepo.updateEntity(ES_INDEX, id, updated);
            entityMetadataService.createEntityMetadata(
                    updated,
                    "update",
                    "success",
                    esWriteMillis,
                    mongoWriteMillis,
                    null
            );
            return updated;
        } catch (Exception ex) {
            boolean isInvalidData = false;
            Throwable cause = ex;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            String rootClass = cause.getClass().getSimpleName();
            String msg       = cause.getMessage() == null
                    ? ""
                    : cause.getMessage().toLowerCase();

            // 2) bucket by root exception type or message
            String reason;
            if ("ResponseException".equals(rootClass)
                    || msg.contains("429")
                    || msg.contains("too many requests")) {
                reason = "HTTP429";
            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
                    || msg.contains("connect timed out")) {
                reason = "ConnectTimeout";
            } else if ("SocketTimeoutException".equals(rootClass)
                    || msg.contains("timeout on connection")
                    || msg.contains("read timeout")) {
                reason = "ReadTimeout";
            } else {
                // any other root cause (e.g. some other IO error)
                reason = rootClass;
            }
            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
                isInvalidData = true;
            }
            EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
                    updated, "update", "failure",
                    null, mongoWriteMillis, reason
            );
            if (!isInvalidData) {
                EntityEvent entityEvent = buildEvent("update",updated,entityMetadata.getMetaId());
                kafka.send(dlqTopic, updated.getId(), entityEvent);
            }
            throw ex ;
        }
    }

    public boolean deleteEntity(String id) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity toUpdate = EntityUtil.markDeleted(existing);
        long mongoWriteMillis = System.currentTimeMillis();
        Entity updated = mongoRepo.updateEntity(toUpdate);
        try {
            long esWriteMillis = System.currentTimeMillis();
            esRepo.updateEntity(ES_INDEX, id, updated);
            entityMetadataService.createEntityMetadata(
                    updated,
                    "delete",
                    "success",
                    esWriteMillis,
                    mongoWriteMillis,
                    null
            );
            return true;}
        catch (Exception ex) {
            boolean isInvalidData = false;
            Throwable cause = ex;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            String rootClass = cause.getClass().getSimpleName();
            String msg       = cause.getMessage() == null
                    ? ""
                    : cause.getMessage().toLowerCase();
            String reason;
            if ("ResponseException".equals(rootClass)
                    || msg.contains("429")
                    || msg.contains("too many requests")) {
                reason = "HTTP429";
            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
                    || msg.contains("connect timed out")) {
                reason = "ConnectTimeout";
            } else if ("SocketTimeoutException".equals(rootClass)
                    || msg.contains("timeout on connection")
                    || msg.contains("read timeout")) {
                reason = "ReadTimeout";
            } else {
                reason = rootClass;
            }
            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
                isInvalidData = true;
            }
            EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
                    existing, "delete", "failure",
                    null, mongoWriteMillis, reason
            );
            if (!isInvalidData) {
                EntityEvent entityEvent = buildEvent("delete",existing,entityMetadata.getMetaId());
                kafka.send(dlqTopic, existing.getId(), entityEvent);
            }
            throw ex ;
        }
    }

    private EntityEvent buildEvent(String op,
                                   Entity entity,
                                   String metadataId ) {
        return EntityEvent.builder()
                .entity(entity)
                .operation(op)
                .metadataId(metadataId)
                .retryCount(0)
                .build();
    }

    private String extractReason(Exception ex) {
        if (ex instanceof ElasticsearchException ee) {
            return ee.error().reason();
        }
        return ex.getMessage();
    }
}
