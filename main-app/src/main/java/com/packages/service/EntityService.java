package com.packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.repository.EntityMongoRepository;
import com.packages.util.EntityUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class EntityService {
    private static final String ES_INDEX = "entity";

    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataService entityMetadataService;

    @Autowired
    public EntityService(
            EntityMongoRepository mongoRepo,
            EntityElasticRepository esRepo,
            EntityMetadataService entityMetadataService
    ) {
        this.mongoRepo = mongoRepo;
        this.esRepo = esRepo;
        this.entityMetadataService = entityMetadataService;
    }

    public Entity createEntity(Entity ent) {
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = new Entity(null, ent.getName(), now, now, null);
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
        } catch (Exception ex) {
            entityMetadataService.createEntityMetadata(
                    saved,
                    "create",
                    "failure",
                    null,
                    mongoWriteMillis,
                    ex.getMessage()
            );
            throw ex;
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
            esRepo.updateEntity(ES_INDEX, id, updated, updated.getCreateTime());
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
            entityMetadataService.createEntityMetadata(
                    updated,
                    "update",
                    "failure",
                    null,
                    mongoWriteMillis,
                    ex.getMessage()
            );
            throw ex;
        }
    }

    public boolean deleteEntity(String id) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        long mongoWriteMillis = System.currentTimeMillis();
        boolean deletedInMongo = mongoRepo.deleteEntity(id);
        if (!deletedInMongo) {
            return false;
        }
        try {
            boolean deletedInEs = esRepo.deleteEntity(ES_INDEX, id);
            long esWriteMillis = System.currentTimeMillis();
            entityMetadataService.createEntityMetadata(
                    existing,
                    "delete",
                    deletedInEs ? "success" : "not_found",
                    deletedInEs ? esWriteMillis : null,
                    mongoWriteMillis,
                    deletedInEs ? null : "ES document not found"
            );
            return deletedInEs;
        } catch (Exception ex) {
            entityMetadataService.createEntityMetadata(
                    existing,
                    "delete",
                    "failure",
                    null,
                    mongoWriteMillis,
                    ex.getMessage()
            );
            throw ex;
        }
    }


}
