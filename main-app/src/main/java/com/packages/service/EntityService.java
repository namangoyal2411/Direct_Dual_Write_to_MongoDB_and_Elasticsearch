package com.packages.service;

import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMongoRepository;
import com.packages.util.EntityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class EntityService {
    private static final String ES_INDEX = "entity";
    private final EntityMongoRepository mongoRepo;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataService metadataService;
    private static final Logger log = LoggerFactory.getLogger(EntityService.class);

    public EntityService(
            EntityMongoRepository mongoRepo,
            EntityElasticRepository esRepo,
            EntityMetadataService metadataService
    ) {
        this.mongoRepo = mongoRepo;
        this.esRepo = esRepo;
        this.metadataService = metadataService;
    }

    public Entity createEntity(Entity ent) {
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = new Entity(null, ent.getName(), now, now, false, null);
        long mongoTime = System.currentTimeMillis();
        Entity saved = mongoRepo.createEntity(toSave);
        try {
            esRepo.createEntity(ES_INDEX, saved);
            long esTime=System.currentTimeMillis();
            metadataService.recordSuccess(saved, "create", esTime, mongoTime);
            return saved;
        } catch (Exception ex) {
            metadataService.recordFailure(saved, "create", mongoTime, ex);
            throw ex;
        }
    }

    public Entity updateEntity(String id, Entity ent) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity updated = EntityUtil.updateEntity(ent, existing);
        long mongoTime = System.currentTimeMillis();
        updated = mongoRepo.updateEntity(updated);
        try {
            esRepo.updateEntity(ES_INDEX, id, updated);
            long esTime=System.currentTimeMillis();
            metadataService.recordSuccess(updated, "update", esTime, mongoTime);
            return updated;
        } catch (Exception ex) {
            metadataService.recordFailure(updated, "update", mongoTime, ex);
            throw ex;
        }
    }

    public boolean deleteEntity(String id) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity marked = EntityUtil.markDeleted(existing);
        long mongoTime = System.currentTimeMillis();
        Entity updated = mongoRepo.updateEntity(marked);
        try {
            esRepo.updateEntity(ES_INDEX, id, updated);
            long esTime=System.currentTimeMillis();
            metadataService.recordSuccess(updated, "delete", esTime, mongoTime);
            return true;
        } catch (Exception ex) {
            metadataService.recordFailure(updated, "delete", mongoTime, ex);
            throw ex;
        }
    }
}

//package com.packages.service;
//
//import com.packages.exception.EntityNotFoundException;
//import com.packages.model.Entity;
//import com.packages.repository.EntityElasticRepository;
//import com.packages.repository.EntityMongoRepository;
//import com.packages.util.EntityUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//
//@Service
//public class EntityService {
//    private static final String es_index = "entity";
//    private final EntityMongoRepository mongoRepo;
//    private final EntityElasticRepository esRepo;
//    private final EntityMetadataService entityMetadataService;
//    private static final Logger log = LoggerFactory.getLogger(EntityService.class);
//    @Autowired
//    public EntityService(
//            EntityMongoRepository mongoRepo,
//            EntityElasticRepository esRepo,
//            EntityMetadataService entityMetadataService
//    ) {
//        this.mongoRepo = mongoRepo;
//        this.esRepo = esRepo;
//        this.entityMetadataService = entityMetadataService;
//    }
//
//    public Entity createEntity(Entity ent) {
//        LocalDateTime now = LocalDateTime.now();
//        Entity toSave = new Entity(null, ent.getName(), now, now, false,null);
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity saved = mongoRepo.createEntity(toSave);
//        try {
//            esRepo.createEntity(es_index, saved);
//            long esWriteMillis = System.currentTimeMillis();
//            entityMetadataService.createEntityMetadata(
//                    saved,
//                    "create",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return saved;
//        } catch (Exception ex) {
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                reason = rootClass;
//            }
//
//            entityMetadataService.createEntityMetadata(
//                    saved,
//                    "create",
//                    "failure",
//                    null,
//                    mongoWriteMillis,
//                   reason
//            );
//            throw ex;
//        }
//    }
//
//    public Entity updateEntity(String id, Entity entity) {
//        Entity existing = mongoRepo.getEntity(id)
//                .orElseThrow(() -> new EntityNotFoundException(id));
//        Entity updated = EntityUtil.updateEntity(entity, existing);
//        long mongoWriteMillis = System.currentTimeMillis();
//        updated = mongoRepo.updateEntity(updated);
//        try {
//            esRepo.updateEntity(es_index, id, updated);
//            long esWriteMillis = System.currentTimeMillis();
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "update",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return updated;
//        }catch (Exception ex) {
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                // any other root cause (e.g. some other IO error)
//                reason = rootClass;
//            }
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "update",
//                    "failure",
//                    null,
//                    mongoWriteMillis,
//                    reason
//            );
//            throw ex;
//        }
//    }
//
//    public boolean deleteEntity(String id) {
//        Entity existing = mongoRepo.getEntity(id)
//                .orElseThrow(() -> new EntityNotFoundException(id));
//        Entity toUpdate = EntityUtil.markDeleted(existing);
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity updated = mongoRepo.updateEntity(toUpdate);
//        try {
//            esRepo.updateEntity(es_index, id, updated);
//            long esWriteMillis = System.currentTimeMillis();
//
//
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "delete",
//                    "success",
//                    esWriteMillis,
//                    mongoWriteMillis,
//                    null
//            );
//            return true;
//        }catch (Exception ex) {
//            Throwable cause = ex;
//            while (cause.getCause() != null) {
//                cause = cause.getCause();
//            }
//            String rootClass = cause.getClass().getSimpleName();
//            String msg       = cause.getMessage() == null
//                    ? ""
//                    : cause.getMessage().toLowerCase();
//            String reason;
//            if ("ResponseException".equals(rootClass)
//                    || msg.contains("429")
//                    || msg.contains("too many requests")) {
//                reason = "HTTP429";
//            } else if ("ConnectionRequestTimeoutException".equals(rootClass)
//                    || msg.contains("connect timed out")) {
//                reason = "ConnectTimeout";
//            } else if ("SocketTimeoutException".equals(rootClass)
//                    || msg.contains("timeout on connection")
//                    || msg.contains("read timeout")) {
//                reason = "ReadTimeout";
//            } else {
//                log.warn("Unclassified ES error: {} – {}", rootClass, cause.getMessage());
//                reason = rootClass;
//            }
//            entityMetadataService.createEntityMetadata(
//                    updated,
//                    "delete",
//                    "failure",
//                    null,
//                    mongoWriteMillis,
//                   reason
//            );
//            throw ex;
//        }
//    }
//
//
//}
