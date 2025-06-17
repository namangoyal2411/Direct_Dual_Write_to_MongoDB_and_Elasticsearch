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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
@Service
public class HybridSyncService {
    EntityMongoRepository entityMongoRepository;
    EntityMetadataRepository entityMetadataRepository;
    EntityElasticRepository entityElasticRepository;
    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;

    public HybridSyncService(EntityMongoRepository entityMongoRepository, EntityMetadataRepository entityMetadataRepository, EntityElasticRepository entityElasticRepository, KafkaTemplate<String, EntityEvent> kafkaTemplate) {
        this.entityMongoRepository = entityMongoRepository;
        this.entityMetadataRepository = entityMetadataRepository;
        this.entityElasticRepository = entityElasticRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    public EntityDTO createEntity(EntityDTO entityDTO) {
        LocalDateTime localDateTime = LocalDateTime.now();
        String indexName = "entity";
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        long mongoWriteMillis = System.currentTimeMillis();
        entityMongoRepository.createEntity(entity);
        long operationSeq = entityMongoRepository.nextSequence(entity.getId());
        entityDTO.setId(entity.getId());
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .approach("Hybrid Sync")
                .operation("create")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        try {
            entityElasticRepository.createEntity(indexName, entity);
            metadata.setEsStatus("success");
            metadata.setEsSyncMillis(System.currentTimeMillis());
            entityMetadataRepository.save(metadata);
            return entityDTO;
        } catch (Exception e) {
            if (metadata.getFirstFailureTime() == null) {
                metadata.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason;
            if (e instanceof ElasticsearchException ee) {
                reason = ee.error().reason();
            } else {
                reason = e.getMessage();
            }
            if (e instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500) {
                metadata.setEsStatus("failure");
                metadata.setDlqReason(reason);
                metadata.setSyncAttempt(1);
                metadata.setEsSyncMillis(null);
                entityMetadataRepository.save(metadata);
                return entityDTO;
            }
            metadata.setEsStatus("failure");
            metadata.setDlqReason(reason);
            metadata.setSyncAttempt(1);
            metadata.setEsSyncMillis(null);
            entityMetadataRepository.save(metadata);
            EntityEvent entityEvent = EntityEventmapper("create", entity, entity.getId(), indexName, metadata);
            sendToDLQ(entityEvent, e);
        }
        return entityDTO;
    }

    public EntityDTO updateEntity( String documentId,EntityDTO entityDTO) {
        String indexName = "entity";
        Entity mongoEntity = entityMongoRepository
                .getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        LocalDateTime createTime = mongoEntity.getCreateTime();
        LocalDateTime localDateTime = LocalDateTime.now();
        Entity entity = Entity.builder().
                id(documentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.updateEntity(entity);
        long mongoWriteMillis = System.currentTimeMillis();
        entityMongoRepository.updateEntity(entity);
        long operationSeq = entityMongoRepository.nextSequence(entity.getId());
        entityDTO.setId(entity.getId());
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .approach("Hybrid Sync")
                .operation("update")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        try {
            entityElasticRepository.createEntity(indexName, entity);
            metadata.setEsStatus("success");
            metadata.setEsSyncMillis(System.currentTimeMillis());
            entityMetadataRepository.save(metadata);
            return entityDTO;
        } catch (Exception e) {
            if (metadata.getFirstFailureTime() == null) {
                metadata.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason;
            if (e instanceof ElasticsearchException ee) {
                reason = ee.error().reason();
            } else {
                reason = e.getMessage();
            }
            if (e instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500) {
                metadata.setEsStatus("failure");
                metadata.setDlqReason(reason);
                metadata.setSyncAttempt(1);
                metadata.setEsSyncMillis(null);
                entityMetadataRepository.save(metadata);
                return entityDTO;
            }
            metadata.setEsStatus("failure");
            metadata.setDlqReason(reason);
            metadata.setSyncAttempt(1);
            metadata.setEsSyncMillis(null);
            entityMetadataRepository.save(metadata);
            EntityEvent entityEvent = EntityEventmapper("update", entity, entity.getId(), indexName, metadata);
            sendToDLQ(entityEvent, e);
        }
        return entityDTO;
    }

    public boolean deleteEntity(String documentId) {
        long mongoWriteMillis = System.currentTimeMillis();
        String indexName = "entity";
        boolean proceed = entityMongoRepository.deleteEntity(documentId);
        boolean esDeleted = false;
        if (proceed) {
            Entity entity = Entity.builder().id(documentId).build();
            long operationSeq = entityMongoRepository.nextSequence(documentId);
            EntityMetadata entityMetadata = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(documentId)
                    .approach("Hybrid Sync")
                    .operation("delete")
                    .operationSeq(operationSeq)
                    .mongoWriteMillis(mongoWriteMillis)
                    .esSyncMillis(null)
                    .syncAttempt(0)
                    .mongoStatus("success")
                    .esStatus("pending")
                    .dlqReason(null)
                    .build();
            try {
                esDeleted = entityElasticRepository.deleteEntity(indexName, documentId);
                entityMetadata.setEsSyncMillis(System.currentTimeMillis());
                entityMetadata.setEsStatus(esDeleted ? "success" : "not_found");
                entityMetadata.setSyncAttempt(1);
                entityMetadataRepository.save(entityMetadata);
            } catch (Exception e) {
                if (entityMetadata.getFirstFailureTime() == null) {
                    entityMetadata.setFirstFailureTime(System.currentTimeMillis());
                }
                String reason;
                if (e instanceof ElasticsearchException ee) {
                    reason = ee.error().reason();
                } else {
                    reason = e.getMessage();
                }
                if (e instanceof ElasticsearchException ee
                        && ee.status() >= 400 && ee.status() < 500) {
                    entityMetadata.setEsStatus("failure");
                    entityMetadata.setDlqReason(reason);
                    entityMetadata.setSyncAttempt(1);
                    entityMetadata.setEsSyncMillis(null);
                    entityMetadataRepository.save(entityMetadata);
                    return false;
                }
                entityMetadata.setEsStatus("failure");
                entityMetadata.setDlqReason(e.getMessage());
                entityMetadata.setSyncAttempt(1);
                entityMetadata.setEsSyncMillis(null);
                entityMetadataRepository.save(entityMetadata);
                EntityEvent entityEvent = EntityEventmapper("delete", entity, entity.getId(), indexName, entityMetadata);
                sendToDLQ(entityEvent, e);
            }
            return true;
        }
        return false;
    }

    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
        try {
            kafkaTemplate.send("dlq71", failedEvent.getEntity().getId(), failedEvent);
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
        }
    }

    public EntityEvent EntityEventmapper(String operation, Entity entity, String documentId, String indexName, EntityMetadata metadata) {
        EntityEvent entityEvent = EntityEvent.builder()
                .entity(entity)
                .operation(operation)
                .id(documentId)
                .index(indexName)
                .entityMetadata(metadata)
                .build();
        return entityEvent;
    }

}
