package com.Packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class HybridSyncService {
    private static final String ES_INDEX = "entity";

    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metaRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;

    public HybridSyncService(EntityMongoRepository mongoRepo,
                             EntityMetadataRepository metaRepo,
                             EntityElasticRepository esRepo,
                             KafkaTemplate<String, EntityEvent> kafka) {
        this.mongoRepo = mongoRepo;
        this.metaRepo = metaRepo;
        this.esRepo = esRepo;
        this.kafka = kafka;
    }

    public EntityDTO createEntity(EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity e = Entity.builder()
                .id(dto.getId())
                .name(dto.getName())
                .createTime(now)
                .modifiedTime(now)
                .build();
        long mongoTs = System.currentTimeMillis();
        mongoRepo.createEntity(e);
        dto.setId(e.getId());
        EntityMetadata meta = buildMetadata(e.getId(), "create", e.getVersion(), mongoTs);
        EntityEvent dlqEvent = null;
        try {
            esRepo.createEntity(ES_INDEX, e);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return dto;
        } catch (Exception ex) {
            if (meta.getFirstFailureTime() == null) {
                meta.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(1);
            dlqEvent = buildEvent("create", e, e.getId(), ES_INDEX, meta);
            return dto;
        } finally {
            metaRepo.save(meta);
            if (dlqEvent != null) {
                kafka.send("dlq114", meta.getEntityId(), dlqEvent);
            }
        }
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        e.setName(dto.getName());
        e.setModifiedTime(now);
        mongoRepo.updateEntity(e);
        dto.setId(e.getId());
        long mongoTs = System.currentTimeMillis();
        EntityMetadata meta = buildMetadata(e.getId(), "update", e.getVersion(), mongoTs);
        EntityEvent dlqEvent = null;
        try {
            esRepo.createEntity(ES_INDEX, e);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return dto;
        } catch (Exception ex) {
            if (meta.getFirstFailureTime() == null) {
                meta.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(1);
            dlqEvent = buildEvent("update", e, e.getId(), ES_INDEX, meta);
            return dto;
        } finally {
            metaRepo.save(meta);
            if (dlqEvent != null) {
                kafka.send("dlq114", meta.getEntityId(), dlqEvent);
            }
        }
    }

    public boolean deleteEntity(String id) {
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        long mongoTs = System.currentTimeMillis();
        boolean proceeded = mongoRepo.deleteEntity(id);
        if (!proceeded) {
            return false;
        }
        EntityMetadata meta = buildMetadata(id, "delete", e.getVersion() + 1, mongoTs);
        EntityEvent dlqEvent = null;
        try {
            boolean deleted = esRepo.deleteEntity(ES_INDEX, id);
            meta.setEsStatus(deleted ? "success" : "not_found");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setSyncAttempt(1);
            return deleted;
        } catch (Exception ex) {
            if (meta.getFirstFailureTime() == null) {
                meta.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(1);
            dlqEvent = buildEvent("delete", Entity.builder().id(id).build(), id, ES_INDEX, meta);
            return false;
        } finally {
            metaRepo.save(meta);
            if (dlqEvent != null) {
                kafka.send("dlq114", meta.getEntityId(), dlqEvent);
            }
        }
    }

    private EntityMetadata buildMetadata(String entityId,
                                         String operation,
                                         long seq,
                                         long mongoTs) {
        return EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entityId)
                .approach("Hybrid Sync")
                .operation(operation)
                .operationSeq(seq)
                .mongoWriteMillis(mongoTs)
                .esSyncMillis(null)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
    }

    private EntityEvent buildEvent(String op,
                                   Entity entity,
                                   String id,
                                   String index,
                                   EntityMetadata meta) {
        return EntityEvent.builder()
                .entity(entity)
                .operation(op)
                .id(id)
                .index(index)
                .entityMetadata(meta)
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
