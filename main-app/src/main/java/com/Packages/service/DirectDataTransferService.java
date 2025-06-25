package com.Packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityMongoRepository;
import com.Packages.repositoryinterface.EntityDirectDataTransferRepository;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DirectDataTransferService {
    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository              esRepo;
    private final EntityMetadataRepository             metaRepo;
    private static final String ES_INDEX   = "entity";

    public DirectDataTransferService(
            EntityMongoRepository mongoRepo,
            EntityElasticRepository esRepo,
            EntityMetadataRepository metaRepo
    ) {
        this.mongoRepo = mongoRepo;
        this.esRepo    = esRepo;
        this.metaRepo  = metaRepo;
    }

    public EntityDTO createEntity(EntityDTO dto) {
        long writeTs     = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.now();
        Entity e = new Entity(
                dto.getId(),
                dto.getName(),
                now,
                now,
                null
        );
        e = mongoRepo.createEntity(e);
        long operationSeq = e.getVersion();

        dto.setId(e.getId());
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(e.getId())
                .approach("Direct Data Transfer")
                .operation("create")
                .operationSeq(operationSeq)
                .mongoWriteMillis(writeTs)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .build();
        try {
            esRepo.createEntity(ES_INDEX, e);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return dto;
        } catch (Exception ex) {
            meta.setEsStatus("failure");
            String reason = (ex instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500)
                    ? ee.error().reason()
                    : ex.getMessage();
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        long writeTs = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.now();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        e.setName(dto.getName());
        e.setModifiedTime(now);
        e = mongoRepo.updateEntity(e);
        long operationSeq = e.getVersion();
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Direct Data Transfer")
                .operation("update")
                .operationSeq(operationSeq)
                .mongoWriteMillis(writeTs)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .build();
        try {
            esRepo.updateEntity(ES_INDEX, id, e, e.getCreateTime());
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
        } catch (Exception ex) {
            meta.setEsStatus("failure");
            String reason = (ex instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500)
                    ? ee.error().reason()
                    : ex.getMessage();
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }

        return dto;
    }

    public boolean deleteEntity(String id) {
        long writeTs = System.currentTimeMillis();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        long operationSeq = e.getVersion();
        mongoRepo.deleteEntity(id);
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Direct Data Transfer")
                .operation("delete")
                .operationSeq(operationSeq)
                .mongoWriteMillis(writeTs)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .build();
        try {
            boolean deleted = esRepo.deleteEntity(ES_INDEX, id);
            meta.setEsStatus(deleted ? "success" : "not_found");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return deleted;
        } catch (Exception ex) {
            meta.setEsStatus("failure");
            String reason = (ex instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500)
                    ? ee.error().reason()
                    : ex.getMessage();
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }
    }
}
