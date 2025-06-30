package com.packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.repository.EntityMongoRepository;
import com.packages.util.EntityUtil;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.SocketTimeoutException;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;
@Service
public class EntityService {
    private static final String ES_INDEX = "entity";

    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataService entityMetadataService;
    private static final Logger log = LoggerFactory.getLogger(EntityService.class);
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
            // normalize to lowercase (null-safe)
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

            entityMetadataService.createEntityMetadata(
                    saved,
                    "create",
                    "failure",
                    null,
                    mongoWriteMillis,
                   reason
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
        }catch (Exception ex) {
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
            entityMetadataService.createEntityMetadata(
                    updated,
                    "update",
                    "failure",
                    null,
                    mongoWriteMillis,
                    reason
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
        }catch (Exception ex) {
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
                log.warn("Unclassified ES error: {} – {}", rootClass, cause.getMessage());
                reason = rootClass;
            }
            entityMetadataService.createEntityMetadata(
                    existing,
                    "delete",
                    "failure",
                    null,
                    mongoWriteMillis,
                   reason
            );
            throw ex;
        }
    }


}
