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
    String dlqTopic ="dlq130";
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

//package com.Packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.Packages.dto.EntityDTO;
//import com.Packages.exception.EntityNotFoundException;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityEvent;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import com.Packages.repository.EntityMongoRepository;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//@Service
//public class HybridSyncService {
//    EntityMongoRepository entityMongoRepository;
//    EntityMetadataRepository entityMetadataRepository;
//    EntityElasticRepository entityElasticRepository;
//    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
//
//    public HybridSyncService(EntityMongoRepository entityMongoRepository, EntityMetadataRepository entityMetadataRepository, EntityElasticRepository entityElasticRepository, KafkaTemplate<String, EntityEvent> kafkaTemplate) {
//        this.entityMongoRepository = entityMongoRepository;
//        this.entityMetadataRepository = entityMetadataRepository;
//        this.entityElasticRepository = entityElasticRepository;
//        this.kafkaTemplate = kafkaTemplate;
//    }
//
//    public EntityDTO createEntity(EntityDTO entityDTO) {
//        LocalDateTime localDateTime = LocalDateTime.now();
//        String indexName = "entity";
//        Entity entity = Entity.builder().
//                id(entityDTO.getId()).
//                name(entityDTO.getName()).
//                createTime(localDateTime).
//                modifiedTime(localDateTime).
//                build();
//        long mongoWriteMillis = System.currentTimeMillis();
//        entityMongoRepository.createEntity(entity);
//        long operationSeq = entity.getVersion();
//        entityDTO.setId(entity.getId());
//        EntityMetadata metadata = EntityMetadata.builder()
//                .metaId(UUID.randomUUID().toString())
//                .entityId(entity.getId())
//                .approach("Hybrid Sync")
//                .operation("create")
//                .operationSeq(operationSeq)
//                .mongoWriteMillis(mongoWriteMillis)
//                .esSyncMillis(null)
//                .syncAttempt(1)
//                .mongoStatus("success")
//                .esStatus("pending")
//                .dlqReason(null)
//                .build();
//        try {
//            entityElasticRepository.createEntity(indexName, entity);
//            metadata.setEsStatus("success");
//            metadata.setEsSyncMillis(System.currentTimeMillis());
//           // entityMetadataRepository.save(metadata);
//            return entityDTO;
//        } catch (Exception e) {
//            if (metadata.getFirstFailureTime() == null) {
//                metadata.setFirstFailureTime(System.currentTimeMillis());
//            }
//            String reason;
//            if (e instanceof ElasticsearchException ee) {
//                reason = ee.error().reason();
//            } else {
//                reason = e.getMessage();
//            }
//            if (e instanceof ElasticsearchException ee
//                    && ee.status() >= 400 && ee.status() < 500) {
//                metadata.setEsStatus("failure");
//                metadata.setDlqReason(reason);
//                metadata.setSyncAttempt(1);
//                metadata.setEsSyncMillis(null);
//                //entityMetadataRepository.save(metadata);
//                return entityDTO;
//            }
//            metadata.setEsStatus("failure");
//            metadata.setDlqReason(reason);
//            metadata.setSyncAttempt(1);
//            metadata.setEsSyncMillis(null);
//            //entityMetadataRepository.save(metadata);
//            EntityEvent entityEvent = EntityEventmapper("create", entity, entity.getId(), indexName, metadata);
//            sendToDLQ(entityEvent, e);
//        }
//        finally {
//            entityMetadataRepository.save(metadata);
//        }
//        return entityDTO;
//    }
//
//    public EntityDTO updateEntity( String documentId,EntityDTO entityDTO) {
//        String indexName = "entity";
//        long mongoWriteMillis = System.currentTimeMillis();
//        LocalDateTime now = LocalDateTime.now();
//        Entity entity = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        entity.setName(entityDTO.getName());
//        entity.setModifiedTime(now);
//        entity = entityMongoRepository.updateEntity(entity);
//        long operationSeq = entity.getVersion();
//        entityDTO.setId(entity.getId());
//        EntityMetadata metadata = EntityMetadata.builder()
//                .metaId(UUID.randomUUID().toString())
//                .entityId(entity.getId())
//                .approach("Hybrid Sync")
//                .operation("update")
//                .operationSeq(operationSeq)
//                .mongoWriteMillis(mongoWriteMillis)
//                .esSyncMillis(null)
//                .syncAttempt(1)
//                .mongoStatus("success")
//                .esStatus("pending")
//                .dlqReason(null)
//                .build();
//        try {
//            entityElasticRepository.createEntity(indexName, entity);
//            metadata.setEsStatus("success");
//            metadata.setEsSyncMillis(System.currentTimeMillis());
//           // entityMetadataRepository.save(metadata);
//            return entityDTO;
//        } catch (Exception e) {
//            if (metadata.getFirstFailureTime() == null) {
//                metadata.setFirstFailureTime(System.currentTimeMillis());
//            }
//            String reason;
//            if (e instanceof ElasticsearchException ee) {
//                reason = ee.error().reason();
//            } else {
//                reason = e.getMessage();
//            }
//            if (e instanceof ElasticsearchException ee
//                    && ee.status() >= 400 && ee.status() < 500) {
//                metadata.setEsStatus("failure");
//                metadata.setDlqReason(reason);
//                metadata.setSyncAttempt(1);
//                metadata.setEsSyncMillis(null);
//               // entityMetadataRepository.save(metadata);
//                return entityDTO;
//            }
//            metadata.setEsStatus("failure");
//            metadata.setDlqReason(reason);
//            metadata.setSyncAttempt(1);
//            metadata.setEsSyncMillis(null);
//            //entityMetadataRepository.save(metadata);
//            EntityEvent entityEvent = EntityEventmapper("update", entity, entity.getId(), indexName, metadata);
//            sendToDLQ(entityEvent, e);
//        }
//        finally {
//            entityMetadataRepository.save(metadata);
//        }
//        return entityDTO;
//    }
//
//    public boolean deleteEntity(String documentId) {
//        long mongoWriteMillis = System.currentTimeMillis();
//        String indexName = "entity";
//        Entity en = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        long operationSeq = en.getVersion()+1;
//        boolean proceed=entityMongoRepository.deleteEntity(documentId);
//        boolean esDeleted = false;
//        if (proceed) {
//            Entity entity = Entity.builder().id(documentId).build();
//            EntityMetadata entityMetadata = EntityMetadata.builder()
//                    .metaId(UUID.randomUUID().toString())
//                    .entityId(documentId)
//                    .approach("Hybrid Sync")
//                    .operation("delete")
//                    .operationSeq(operationSeq)
//                    .mongoWriteMillis(mongoWriteMillis)
//                    .esSyncMillis(null)
//                    .syncAttempt(0)
//                    .mongoStatus("success")
//                    .esStatus("pending")
//                    .dlqReason(null)
//                    .build();
//            try {
//                esDeleted = entityElasticRepository.deleteEntity(indexName, documentId);
//                entityMetadata.setEsSyncMillis(System.currentTimeMillis());
//                entityMetadata.setEsStatus(esDeleted ? "success" : "not_found");
//                entityMetadata.setSyncAttempt(1);
//                //entityMetadataRepository.save(entityMetadata);
//            } catch (Exception e) {
//                if (entityMetadata.getFirstFailureTime() == null) {
//                    entityMetadata.setFirstFailureTime(System.currentTimeMillis());
//                }
//                String reason;
//                if (e instanceof ElasticsearchException ee) {
//                    reason = ee.error().reason();
//                } else {
//                    reason = e.getMessage();
//                }
//                if (e instanceof ElasticsearchException ee
//                        && ee.status() >= 400 && ee.status() < 500) {
//                    entityMetadata.setEsStatus("failure");
//                    entityMetadata.setDlqReason(reason);
//                    entityMetadata.setSyncAttempt(1);
//                    entityMetadata.setEsSyncMillis(null);
//                 //   entityMetadataRepository.save(entityMetadata);
//                    return false;
//                }
//                entityMetadata.setEsStatus("failure");
//                entityMetadata.setDlqReason(e.getMessage());
//                entityMetadata.setSyncAttempt(1);
//                entityMetadata.setEsSyncMillis(null);
//               // entityMetadataRepository.save(entityMetadata);
//                EntityEvent entityEvent = EntityEventmapper("delete", entity, entity.getId(), indexName, entityMetadata);
//                sendToDLQ(entityEvent, e);
//            }
//            finally {
//                entityMetadataRepository.save(entityMetadata);
//            }
//            return true;
//        }
//        return false;
//    }
//    // keep different topic while running this from kafka sync
//    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
//        try {
//            kafkaTemplate.send("dlq114", failedEvent.getEntity().getId(), failedEvent);
//        } catch (Exception ex) {
//            System.err.println("Failed to send to DLQ: " + ex.getMessage());
//        }
//    }
//
//    public EntityEvent EntityEventmapper(String operation, Entity entity, String documentId, String indexName, EntityMetadata metadata) {
//        EntityEvent entityEvent = EntityEvent.builder()
//                .entity(entity)
//                .operation(operation)
//                .id(documentId)
//                .index(indexName)
//                .entityMetadata(metadata)
//                .build();
//        return entityEvent;
//    }
//
//}
/// / should i send dlq also in finally
