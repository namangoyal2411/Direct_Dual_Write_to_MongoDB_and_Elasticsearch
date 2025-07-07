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

@Service
public class EntityService {

    private static final String ES_INDEX  = "entity";
    private static final String DLQ_TOPIC = "dlq175";

    private final EntityMongoRepository      mongoRepo;
    private final EntityElasticRepository    esRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;
    private final EntityMetadataService      metadataService;

    public EntityService(EntityMongoRepository mongoRepo,
                         EntityElasticRepository esRepo,
                         KafkaTemplate<String, EntityEvent> kafka,
                         EntityMetadataService metadataService) {

        this.mongoRepo       = mongoRepo;
        this.esRepo          = esRepo;
        this.kafka           = kafka;
        this.metadataService = metadataService;
    }

    /*────────────────────────────  CREATE  ────────────────────────────*/

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
            throw ex;         // let controller know
        }
    }

    /*────────────────────────────  UPDATE  ────────────────────────────*/

    public Entity updateEntity(String id, Entity delta) {

        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));

        Entity updated       = mongoRepo.updateEntity( EntityUtil.updateEntity(delta, existing) );
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

    /*────────────────────────────  DELETE  ────────────────────────────*/

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

    /*────────────────────────  COMMON HELPERS  ────────────────────────*/

    private void handleFailure(String op,
                               Entity entity,
                               long mongoWriteMs,
                               Exception ex) {

        boolean isInvalidData = ex instanceof ElasticsearchException ee
                && ee.status() == 400;

        String reason = classify(ex);

        EntityMetadata meta = metadataService.createEntityMetadata(
                entity, op, "failure", null, mongoWriteMs, reason
        );

        if (!isInvalidData) {
            EntityEvent evt = buildEvent(op, entity, meta.getMetaId());
            kafka.send(DLQ_TOPIC, entity.getId(), evt);
        }
    }

    private String classify(Exception ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) cause = cause.getCause();

        String root = cause.getClass().getSimpleName();
        String msg  = cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();

        if ("ResponseException".equals(root) || msg.contains("429") || msg.contains("too many requests"))
            return "HTTP429";
        if ("ConnectionRequestTimeoutException".equals(root) || msg.contains("connect timed out"))
            return "ConnectTimeout";
        if ("SocketTimeoutException".equals(root) ||
                msg.contains("timeout on connection") || msg.contains("read timeout"))
            return "ReadTimeout";

        return root;
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

//package com.packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.packages.exception.EntityNotFoundException;
//import com.packages.model.Entity;
//import com.packages.model.EntityEvent;
//import com.packages.model.EntityMetadata;
//import com.packages.repository.EntityElasticRepository;
//import com.packages.repository.EntityMetadataRepository;
//import com.packages.repository.EntityMongoRepository;
//import com.packages.util.EntityUtil;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Service
//public class EntityService {
//    private static final String ES_INDEX = "entity";
//    private final EntityMongoRepository mongoRepo;
//    private final EntityElasticRepository esRepo;
//    private final KafkaTemplate<String, EntityEvent> kafka;
//    private final EntityMetadataService entityMetadataService;
//    public EntityService(EntityMongoRepository mongoRepo,
//                         EntityElasticRepository esRepo,
//                         KafkaTemplate<String, EntityEvent> kafka,EntityMetadataService entityMetadataService) {
//        this.mongoRepo = mongoRepo;
//        this.esRepo = esRepo;
//        this.kafka = kafka;
//        this.entityMetadataService = entityMetadataService;
//    }
//    String dlqTopic ="dlq170";
//    public Entity createEntity(Entity ent) {
//        LocalDateTime now = LocalDateTime.now();
//        Entity toSave = new Entity(null, ent.getName(), now, now,false, null);
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity saved = mongoRepo.createEntity(toSave);
//        try {
//
//            esRepo.createEntity(ES_INDEX, saved);
//            long esWriteMillis = System.currentTimeMillis();
//            entityMetadataService.createEntityMetadata(
//                    saved,
//                    "create",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return saved;
//        }  catch (Exception ex) {
//            boolean isInvalidData = false;
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                reason = rootClass;
//            }
//            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
//                isInvalidData = true;
//            }
//           EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
//                    saved, "create", "failure",
//                    null, mongoWriteMillis, reason
//            );
//            if (!isInvalidData) {
//                EntityEvent entityEvent = buildEvent("create",saved,entityMetadata.getMetaId());
//                kafka.send(dlqTopic, saved.getId(), entityEvent);
//            }
//        throw ex ;
//        }
//
//    }
//
//    public Entity updateEntity(String id, Entity entity) {
//        Entity existing = mongoRepo.getEntity(id)
//                .orElseThrow(() -> new EntityNotFoundException(id));
//        Entity updated = EntityUtil.updateEntity(entity, existing);
//        long mongoWriteMillis = System.currentTimeMillis();
//        updated = mongoRepo.updateEntity(updated);
//        try {
//            esRepo.updateEntity(ES_INDEX, id, updated);
//            long esWriteMillis = System.currentTimeMillis();
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "update",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return updated;
//        } catch (Exception ex) {
//            boolean isInvalidData = false;
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                // any other root cause (e.g. some other IO error)
//                reason = rootClass;
//            }
//            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
//                isInvalidData = true;
//            }
//            EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
//                    updated, "update", "failure",
//                    null, mongoWriteMillis, reason
//            );
//            if (!isInvalidData) {
//                EntityEvent entityEvent = buildEvent("update",updated,entityMetadata.getMetaId());
//                kafka.send(dlqTopic, updated.getId(), entityEvent);
//            }
//            throw ex ;
//        }
//    }
//
//    public boolean deleteEntity(String id) {
//        Entity existing = mongoRepo.getEntity(id)
//                .orElseThrow(() -> new EntityNotFoundException(id));
//        Entity toUpdate = EntityUtil.markDeleted(existing);
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity updated = mongoRepo.updateEntity(toUpdate);
//        try {
//            esRepo.updateEntity(ES_INDEX, id, updated);
//            long esWriteMillis = System.currentTimeMillis();
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "delete",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return true;}
//        catch (Exception ex) {
//            boolean isInvalidData = false;
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                reason = rootClass;
//            }
//            if (ex instanceof ElasticsearchException ee && ee.status() == 400) {
//                isInvalidData = true;
//            }
//            EntityMetadata entityMetadata= entityMetadataService.createEntityMetadata(
//                    existing, "delete", "failure",
//                    null, mongoWriteMillis, reason
//            );
//            if (!isInvalidData) {
//                EntityEvent entityEvent = buildEvent("delete",existing,entityMetadata.getMetaId());
//                kafka.send(dlqTopic, existing.getId(), entityEvent);
//            }
//            throw ex ;
//        }
//    }
//
//    private EntityEvent buildEvent(String op,
//                                   Entity entity,
//                                   String metadataId ) {
//        return EntityEvent.builder()
//                .entity(entity)
//                .operation(op)
//                .metadataId(metadataId)
//                .retryCount(0)
//                .build();
//    }
//
//    private String extractReason(Exception ex) {
//        if (ex instanceof ElasticsearchException ee) {
//            return ee.error().reason();
//        }
//        return ex.getMessage();
//    }
//}
