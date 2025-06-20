package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityMongoRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ChangeStreamService {
    private final EntityMongoRepository entityMongoRepository;
    private final EntityMetadataRepository entityMetadataRepository;
    private final MongoClient mongoClient;
    @Autowired
    public ChangeStreamService(EntityMongoRepository entityMongoRepository,
                               EntityMetadataRepository entityMetadataRepository,MongoClient mongoClient) {
        this.entityMongoRepository = entityMongoRepository;
        this.entityMetadataRepository = entityMetadataRepository;
        this.mongoClient = mongoClient;
    }
    public EntityDTO createEntity(EntityDTO entityDTO) {
        long mongoWriteMillis = System.currentTimeMillis();
        String indexName = "entity";
        String metadataId = UUID.randomUUID().toString();
        LocalDateTime localDateTime = LocalDateTime.now();
        Entity entity = Entity.builder()
                .id(entityDTO.getId())
                .name(entityDTO.getName())
                .createTime(localDateTime)
                .modifiedTime(localDateTime)
                .metadataId(metadataId)
                .build();
        long operationSeq = entityMongoRepository.nextSequence(entity.getId());
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(metadataId)
                .entityId(entity.getId())
                .approach("ChangeStream")
                .operation("create")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        entityMetadataRepository.save(metadata);
        entityMongoRepository.createEntity(entity);
        return entityDTO;
    }

    public EntityDTO updateEntity(String documentId, EntityDTO entityDTO) {
        long mongoWriteMillis = System.currentTimeMillis();
        Entity mongoEntity = entityMongoRepository
                .getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        LocalDateTime createTime = mongoEntity.getCreateTime();
        LocalDateTime localDateTime = LocalDateTime.now();
        String metadataId = UUID.randomUUID().toString();
        Entity entity = Entity.builder()
                .id(documentId)
                .name(entityDTO.getName())
                .createTime(createTime)
                .modifiedTime(localDateTime)
                .metadataId(metadataId)
                .build();
        long operationSeq = entityMongoRepository.nextSequence(entity.getId());
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(metadataId)
                .entityId(entity.getId())
                .approach("ChangeStream")
                .operation("update")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        entityMetadataRepository.save(metadata);
        entityMongoRepository.updateEntity(entity);
        return entityDTO;
    }
    public boolean deleteEntity(String documentId) {
        long mongoWriteMillis = System.currentTimeMillis();
        String metadataId = UUID.randomUUID().toString();
        long operationSeq = entityMongoRepository.nextSequence(documentId);
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(metadataId)
                .entityId(documentId)
                .approach("ChangeStream")
                .operation("delete")
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
        entityMetadataRepository.save(metadata);
        Entity e = entityMongoRepository.getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        e.setMetadataId(metadataId);
        entityMongoRepository.updateEntity(e);

        boolean mongoDeleted = entityMongoRepository.deleteEntity(documentId);
        return mongoDeleted;
    }
}
