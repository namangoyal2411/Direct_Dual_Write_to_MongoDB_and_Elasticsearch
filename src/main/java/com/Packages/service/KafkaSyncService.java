package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.kafka.EntityProducer;
import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class KafkaSyncService {
    EntityMongoRepository entityMongoRepository;
    EntityMetadataRepository entityMetadataRepository;
    @Autowired
    public KafkaSyncService(EntityMongoRepository entityMongoRepository,EntityMetadataRepository entityMetadataRepository) {
        this.entityMongoRepository = entityMongoRepository;
        this.entityMetadataRepository= entityMetadataRepository;
    }
    @Autowired
    private EntityProducer kafkaProducer;
    String service = "Kafka Sync";
    public EntityDTO createEntity(EntityDTO entityDTO){
        String indexName="entity";
        LocalDateTime localDateTime= LocalDateTime.now();
        long mongoWriteMillis = System.currentTimeMillis();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.createEntity(entity);
        long previousOpSeq = entityMetadataRepository.getLatestOperationSeq(entity.getId(),service);
        long operationSeq = previousOpSeq + 1;
        System.out.println(operationSeq);
        EntityMetadata entityMetadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .approach("Kafka Sync")
                .operation("create")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        entityMetadataRepository.save(entityMetadata);
        EntityEvent entityEvent = EntityEventmapper("create",entity, entity.getId(), indexName,entityMetadata.getMetaId());
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public EntityDTO updateEntity(String documentId,EntityDTO entityDTO){
        String indexName="entity";
        long mongoWriteMillis = System.currentTimeMillis();
        Entity mongoEntity = entityMongoRepository
                .getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        LocalDateTime createTime = mongoEntity.getCreateTime();
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(documentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.updateEntity(entity);
        long previousOpSeq = entityMetadataRepository.getLatestOperationSeq(documentId,service);
        long operationSeq = previousOpSeq + 1;
        System.out.println(operationSeq);
        EntityMetadata entityMetadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(documentId)
                .approach("Kafka Sync")
                .operation("update")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        entityMetadataRepository.save(entityMetadata);
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEventmapper("update",entity, entity.getId(), indexName,entityMetadata.getMetaId());
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public boolean deleteEntity(String documentId ){
        String indexName="entity";
        if (entityMongoRepository.deleteEntity(documentId)) {
            Entity entity = Entity.builder().id(documentId).build();
            long previousOpSeq = entityMetadataRepository.getLatestOperationSeq(documentId,service);
            long operationSeq = previousOpSeq + 1;
            EntityMetadata entityMetadata = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(documentId)
                    .approach("Kafka Sync")
                    .operation("delete")
                    .operationSeq(operationSeq)
                    .mongoWriteMillis(System.currentTimeMillis())
                    .esSyncMillis(null)
                    .syncAttempt(0)
                    .mongoStatus("success")
                    .esStatus("pending")
                    .dlqReason(null)
                    .build();
            entityMetadataRepository.save(entityMetadata);
            EntityEvent entityEvent = EntityEventmapper("delete",entity, entity.getId(), indexName,entityMetadata.getMetaId());
            kafkaProducer.sendToKafka(entityEvent);
            return true;
        }
        return false ;
    }
    public EntityEvent EntityEventmapper(String operation,Entity entity, String documentId, String indexName,String entityMetadataId){
        EntityEvent entityEvent= EntityEvent.builder()
                .entity(entity)
                .operation(operation)
                .id(documentId)
                .index(indexName)
                .metadataId(entityMetadataId)
                .build();
        return entityEvent;
    }
}