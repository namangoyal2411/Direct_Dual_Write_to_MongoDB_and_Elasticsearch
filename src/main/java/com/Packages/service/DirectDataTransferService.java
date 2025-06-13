package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;


@Service
public class DirectDataTransferService {
    EntityMongoRepository entityMongoRepository;
    EntityElasticRepository entityElasticRepository;
    EntityMetadataRepository entityMetadataRepository;

    @Autowired
    public DirectDataTransferService(EntityMongoRepository entityMongoRepository, EntityElasticRepository entityElasticRepository, EntityMetadataRepository entityMetadataRepository) {
        this.entityMongoRepository = entityMongoRepository;
        this.entityElasticRepository = entityElasticRepository;
        this.entityMetadataRepository = entityMetadataRepository;
    }
    String service = "Direct Data Transfer";
    public EntityDTO createEntity(EntityDTO entityDTO) {
        String indexName = "entity";
        LocalDateTime localDateTime = LocalDateTime.now();
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
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .approach("Direct Data Transfer")
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
            return entityDTO;
        } catch (Exception e) {
            metadata.setEsStatus("failure");
            metadata.setDlqReason(e.getMessage());
            throw e;
        } finally {
            entityMetadataRepository.save(metadata);
        }

    }

    public EntityDTO updateEntity(String documentId, EntityDTO entityDTO) {
        String indexName = "entity";
        Entity mongoEntity = entityMongoRepository
                .getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        LocalDateTime createTime;
        long mongoWriteMillis = System.currentTimeMillis();
        createTime = mongoEntity.getCreateTime();
        LocalDateTime localDateTime = LocalDateTime.now();
        Entity entity = Entity.builder().
                id(documentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.updateEntity(entity);
        long previousOpSeq = entityMetadataRepository.getLatestOperationSeq(documentId,service);
        long operationSeq = previousOpSeq + 1;
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .approach("Direct Data Transfer")
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
            entityElasticRepository.updateEntity(indexName, documentId, entity, createTime);
            metadata.setEsStatus("success");
            metadata.setEsSyncMillis(System.currentTimeMillis());
        } catch (Exception e) {
            metadata.setEsStatus("failure");
            metadata.setDlqReason(e.getMessage());
            throw e;
        } finally {
            entityMetadataRepository.save(metadata);
        }
        return entityDTO;
    }
    public boolean deleteEntity(String documentId) {
        boolean mongoDeleted=false ;
        boolean esDeleted=false;
        if (entityMongoRepository.deleteEntity(documentId)) {
            mongoDeleted=true;
            String indexName = "entity";
            long previousOpSeq = entityMetadataRepository.getLatestOperationSeq(documentId,service);
            long operationSeq = previousOpSeq + 1;
            EntityMetadata metadata = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(documentId)
                    .approach("Direct Data Transfer")
                    .operation("delete")
                    .operationSeq(operationSeq)
                    .mongoWriteMillis(System.currentTimeMillis())
                    .syncAttempt(1)
                    .mongoStatus("success")
                    .esStatus("pending")
                    .dlqReason(null)
                    .build();
            try {
                esDeleted = entityElasticRepository.deleteEntity(indexName, documentId);
                metadata.setEsSyncMillis(System.currentTimeMillis());
                metadata.setEsStatus(esDeleted ? "success" : "not_found");
            } catch (Exception e) {
                metadata.setEsSyncMillis(System.currentTimeMillis());
                metadata.setEsStatus("failure");
                metadata.setDlqReason(e.getMessage());
                throw e;
            } finally {
                entityMetadataRepository.save(metadata);
            }
        }
        return mongoDeleted && esDeleted;
    }

}

