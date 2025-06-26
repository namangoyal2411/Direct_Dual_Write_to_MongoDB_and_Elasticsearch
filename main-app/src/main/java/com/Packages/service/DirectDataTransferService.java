package com.Packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class DirectDataTransferService {
    private static final String ES_INDEX = "entity";

    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metaRepo;

    public DirectDataTransferService(
            EntityMongoRepository mongoRepo,
            EntityElasticRepository esRepo,
            EntityMetadataRepository metaRepo
    ) {
        this.mongoRepo = mongoRepo;
        this.esRepo = esRepo;
        this.metaRepo = metaRepo;
    }

    public EntityDTO createEntity(EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity e = new Entity(dto.getId(), dto.getName(), now, now, null);
        e = mongoRepo.createEntity(e);
        dto.setId(e.getId());
        EntityMetadata meta = buildMetadata(e.getId(), "create", e.getVersion(),
                System.currentTimeMillis());
        try {
            esRepo.createEntity(ES_INDEX, e);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return dto;
        } catch (Exception ex) {
            // failure path
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        e.setName(dto.getName());
        e.setModifiedTime(now);
        e = mongoRepo.updateEntity(e);
        EntityMetadata meta = buildMetadata(id, "update", e.getVersion(),
                System.currentTimeMillis());
        try {
            esRepo.updateEntity(ES_INDEX, id, e, e.getCreateTime());
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return dto;
        } catch (Exception ex) {
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }
    }

    public boolean deleteEntity(String id) {
        long writeTs = System.currentTimeMillis();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        boolean deletedInMongo = mongoRepo.deleteEntity(id);
        if (!deletedInMongo) return false;
        EntityMetadata meta = buildMetadata(id, "delete", e.getVersion() + 1, writeTs);
        try {
            boolean deletedInEs = esRepo.deleteEntity(ES_INDEX, id);
            meta.setEsStatus(deletedInEs ? "success" : "not_found");
            meta.setEsSyncMillis(System.currentTimeMillis());
            return deletedInEs;
        } catch (Exception ex) {
            String reason = extractReason(ex);
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            throw ex;
        } finally {
            metaRepo.save(meta);
        }
    }

    private EntityMetadata buildMetadata(String entityId,
                                         String operation,
                                         long operationSeq,
                                         long mongoWriteMillis) {
        return EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entityId)
                .approach("Direct Data Transfer")
                .operation(operation)
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .syncAttempt(1)
                .mongoStatus("success")
                .esStatus("pending")
                .build();
    }

    private String extractReason(Exception ex) {
        if (ex instanceof ElasticsearchException ee
                && ee.status() >= 400 && ee.status() < 500) {
            return ee.error().reason();
        }
        return ex.getMessage();
    }
}
