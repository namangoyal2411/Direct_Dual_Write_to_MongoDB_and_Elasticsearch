package com.Packages.Service;

import com.Packages.DTO.EntityDTO;
import com.Packages.Exception.EntityNotFoundException;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityMetadata;
import com.Packages.Repository.EntityElasticRepository;
import com.Packages.Repository.EntityMetadataRepository;
import com.Packages.Repository.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
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
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .operation("create")
                .operationSeq(1L)
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
        entityMongoRepository.createEntity(entity);
        EntityMetadata metadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .operation("update")
                .operationSeq(1L)
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
        entityElasticRepository.updateEntity(indexName, documentId, entity, createTime);
        return entityDTO;
    }
    public boolean deleteEntity(String documentId) {
        boolean mongoDeleted=false ;
        boolean esDeleted=false;
        if (entityMongoRepository.deleteEntity(documentId)) {
             mongoDeleted = entityMongoRepository.deleteEntity(documentId);
            String indexName = "entity";
            EntityMetadata metadata = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(documentId)
                    .operation("delete")
                    .operationSeq(1L)
                    .mongoWriteMillis(System.currentTimeMillis())
                    .syncAttempt(1)
                    .mongoStatus(mongoDeleted ? "success" : "not_found")
                    .esStatus("pending")
                    .dlqReason(null)
                    .build();
            esDeleted = false;
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

