package com.packages.service;

import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityMetadataMongoRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.util.EntityMetadataUtil;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class EntityMetadataService {

    private final EntityMetadataRepository repo;
    private final EntityMetadataMongoRepository mongoRepo;

    @Autowired
    public EntityMetadataService(EntityMetadataRepository repo, EntityMetadataMongoRepository mongoRepo) {
        this.repo = repo;
        this.mongoRepo = mongoRepo;
    }

    public EntityMetadata createEntityMetadata(Entity entity,
                                               String operation,
                                               String status,
                                               Long esWriteTime,
                                               Long mongoWriteMillis,
                                               Exception ex) {
        LocalDateTime modified = entity.getModifiedTime();
        long version = entity.getVersion();
        String metaId = entity.getId() + "-" + operation + "-" + version;
        String failureReason = classify(ex);
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(metaId)
                .entityId(entity.getId())
                .approach("hybrid kafka sync")
                .operation(operation)
                .operationSeq(version)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(esWriteTime)
                .esStatus(status)
                .failureReason(failureReason)
                .firstFailureTime("failure".equalsIgnoreCase(status)
                        ? System.currentTimeMillis()
                        : null)
                .build();
        try {
            mongoRepo.save(meta);
            repo.save(meta);
        } catch (Exception e) {
            throw e;
        }

        return meta;
    }

    public EntityMetadata updateEntityMetadata(String metaId,
                                               String status,
                                               Long esSyncMillis,
                                               String failureReason) {
        EntityMetadata meta = mongoRepo.getEntityMetadata(metaId)
                .orElseThrow(() -> new IllegalArgumentException("Meta not found: " + metaId));
        EntityMetadataUtil.applyUpdate(meta, status, esSyncMillis, failureReason);

        mongoRepo.save(meta);
        repo.save(meta);

        return meta;
    }

    private String classify(Exception ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) cause = cause.getCause();

        String root = cause.getClass().getSimpleName();
        String msg = cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();

        if ("ResponseException".equals(root) || msg.contains("429") || msg.contains("too many requests"))
            return "HTTP429";
        if ("ConnectionRequestTimeoutException".equals(root) || msg.contains("connect timed out"))
            return "ConnectTimeout";
        if ("SocketTimeoutException".equals(root) ||
                msg.contains("timeout on connection") || msg.contains("read timeout"))
            return "ReadTimeout";

        return root;
    }
}