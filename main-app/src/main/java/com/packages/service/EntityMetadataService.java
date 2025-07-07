package com.packages.service;

import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityMetadataMongoRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.util.EntityMetadataUtil;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class EntityMetadataService {

    private final EntityMetadataRepository repo;
    private final EntityMetadataMongoRepository mongoRepo;
    @Autowired
    public EntityMetadataService(EntityMetadataRepository repo,EntityMetadataMongoRepository mongoRepo) {
        this.repo     = repo;
        this.mongoRepo = mongoRepo;
    }
    public EntityMetadata createEntityMetadata(Entity entity,
                                               String operation,
                                               String status,
                                               Long esWriteTime,
                                               Long mongoWriteMillis,
                                               String failureReason) {
        long version = entity.getVersion();
        String metaId = entity.getId() + "-" + operation + "-" + version;
        EntityMetadata meta = EntityMetadata.builder()
                .metaId(metaId)
                .entityId(entity.getId())
                .approach("Change Stream")
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
            try{
                mongoRepo.save(meta);
                repo.save(meta);
            }
            catch (Exception e ){
                throw e ;
            }
        return meta ;
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
}