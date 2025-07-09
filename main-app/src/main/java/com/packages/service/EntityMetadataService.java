package com.packages.service;

import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityMetadataMongoRepository;
import com.packages.repository.EntityMetadataRepository;
import org.springframework.stereotype.Service;

@Service
public class EntityMetadataService {

    private final EntityMetadataRepository repo;
    private final EntityMetadataMongoRepository mongoRepo;

    public EntityMetadataService(EntityMetadataRepository repo,
                                 EntityMetadataMongoRepository mongoRepo) {
        this.repo = repo;
        this.mongoRepo = mongoRepo;
    }

    public void recordSuccess(Entity entity,
                              String operation,
                              long esSyncMillis,
                              long mongoWriteMillis) {
        createEntityMetadata(entity, operation, "success", esSyncMillis, mongoWriteMillis, null);
    }

    public void recordFailure(Entity entity,
                              String operation,
                              long mongoWriteMillis,
                              Exception ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String rootClass = cause.getClass().getSimpleName();
        String msg = cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();

        String reason;
        if ("ResponseException".equals(rootClass) || msg.contains("429") || msg.contains("too many requests")) {
            reason = "HTTP429";
        } else if ("ConnectionRequestTimeoutException".equals(rootClass) || msg.contains("connect timed out")) {
            reason = "ConnectTimeout";
        } else if ("SocketTimeoutException".equals(rootClass)
                || msg.contains("timeout on connection")
                || msg.contains("read timeout")) {
            reason = "ReadTimeout";
        } else {
            reason = rootClass;
        }

        createEntityMetadata(entity, operation, "failure", null, mongoWriteMillis, reason);
    }

    private void createEntityMetadata(Entity entity,
                                      String operation,
                                      String status,
                                      Long esSyncMillis,
                                      Long mongoWriteMillis,
                                      String failureReason) {
        long version = entity.getVersion();
        String metaId = entity.getId() + "-" + operation + "-" + version;
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(metaId)
                .entityId(entity.getId())
                .approach("Direct Data Transfer")
                .operation(operation)
                .operationSeq(version)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(esSyncMillis)
                .esStatus(status)
                .failureReason(failureReason)
                .firstFailureTime("failure".equalsIgnoreCase(status)
                        ? System.currentTimeMillis()
                        : null)
                .build();

        mongoRepo.save(meta);
        repo.save(meta);
    }
}

