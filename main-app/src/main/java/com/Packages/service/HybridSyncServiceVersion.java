package com.Packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEventVersion;
import com.Packages.model.EntityMetadataversion;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class HybridSyncServiceVersion {

    private final EntityMongoRepository                   mongoRepo;
    private final EntityMetadataRepository                metaRepo;
    private final EntityElasticRepository                 elasticRepo;
    private final KafkaTemplate<String, EntityEventVersion> kafkaTemplate;

    @Autowired
    public HybridSyncServiceVersion(
            EntityMongoRepository mongoRepo,
            EntityMetadataRepository metaRepo,
            EntityElasticRepository elasticRepo,
            @Qualifier("entityEventVersionKafkaTemplate")
            KafkaTemplate<String, EntityEventVersion> kafkaTemplate
    ) {
        this.mongoRepo     = mongoRepo;
        this.metaRepo      = metaRepo;
        this.elasticRepo   = elasticRepo;
        this.kafkaTemplate = kafkaTemplate;
    }
    private static final Logger log = LoggerFactory.getLogger(KafkaSyncServiceVersion.class);

    public EntityDTO createEntity(EntityDTO dto) {
        log.info("→ [Service] updateEntity(id={}, dto={})", dto.getId(), dto);
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = Entity.builder()
                .id(dto.getId())
                .name(dto.getName())
                .createTime(now)
                .modifiedTime(now)
                .build();
        Entity saved = mongoRepo.createEntity(toSave);
        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(saved.getId())
                .approach("Hybrid Sync")
                .operation("create")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);

        long version = saved.getVersion();

        try {
            elasticRepo.createEntityWithVersion("entity", saved.getId(), saved, version);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            metaRepo.updateversion(meta.getMetaId(), meta);
        } catch (ElasticsearchException ee) {
            int status = ee.status();
            String reason = ee.error().reason();
            if (status == 409) {
                meta.setEsStatus("failure");
                meta.setDlqReason("Stale");
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(System.currentTimeMillis());
                metaRepo.updateversion(meta.getMetaId(), meta);
                return dto;
            }
            if (status >= 400 && status < 500) {
                // permanent failure
                meta.setEsStatus("failure");
                meta.setDlqReason(reason);
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(null);
                metaRepo.updateversion(meta.getMetaId(), meta);
                return dto;
            }
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", saved.getId(),
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(saved.getId())
                            .version(version)
                            .entity(saved)
                            .build()
            );
        } catch (IOException ioe) {
            meta.setEsStatus("failure");
            meta.setDlqReason(ioe.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", saved.getId(),
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(saved.getId())
                            .version(version)
                            .entity(saved)
                            .build()
            );
        }
        catch (Exception ex) {
            // transient errors (including your RuntimeException)
            meta.setEsStatus("failure");
            meta.setDlqReason(ex.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);

            kafkaTemplate.send(
                    "dlq110",
                    saved.getId(),
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(saved.getId())
                            .version(version)
                            .entity(saved)
                            .build()
            );
        }
        dto.setId(saved.getId());
        return dto;
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        log.info("→ [Service] updateEntity(id={}, dto={})", id, dto);
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        existing.setName(dto.getName());
        existing.setModifiedTime(LocalDateTime.now());
        Entity fresh = mongoRepo.updateEntity(existing);

        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Hybrid Sync")
                .operation("update")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);

        long version = fresh.getVersion();
        try {
            elasticRepo.updateEntityWithVersion("entity", id, fresh, version);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            metaRepo.updateversion(meta.getMetaId(), meta);
        } catch (ElasticsearchException ee) {
            int status = ee.status();
            String reason = ee.error().reason();
            if (status == 409) {
                meta.setEsStatus("failure");
                meta.setDlqReason("Stale");
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(System.currentTimeMillis());
                metaRepo.updateversion(meta.getMetaId(), meta);
                return dto;
            }
            if (status >= 400 && status < 500) {
                meta.setEsStatus("failure");
                meta.setDlqReason(reason);
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(null);
                metaRepo.updateversion(meta.getMetaId(), meta);
                return dto;
            }
            // transient → DLQ
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", id,
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(id)
                            .version(version)
                            .entity(fresh)
                            .build()
            );
        } catch (IOException ioe) {
            meta.setEsStatus("failure");
            meta.setDlqReason(ioe.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", id,
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(id)
                            .version(version)
                            .entity(fresh)
                            .build()
            );
        }
        catch (Exception ex) {
            // transient errors (including your RuntimeException)
            meta.setEsStatus("failure");
            meta.setDlqReason(ex.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);

            kafkaTemplate.send(
                    "dlq110",
                    existing.getId(),
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(existing.getId())
                            .version(version)
                            .entity(existing)
                            .build()
            );
        }
        dto.setId(id);
        return dto;
    }

    public boolean deleteEntity(String id) {
        // 1) load current entity (to read its version)
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        long version = existing.getVersion()+1;

        // 2) delete from Mongo
        if (!mongoRepo.deleteEntity(id)) {
            return false;
        }

        // 3) write metadata
        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Hybrid Sync")
                .operation("delete")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);
        try {
            elasticRepo.deleteEntityWithVersion("entity", id, version);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            metaRepo.updateversion(meta.getMetaId(), meta);
            return true;
        } catch (ElasticsearchException ee) {
            int status = ee.status();
            String reason = ee.error().reason();
            if (status == 409) {
                meta.setEsStatus("failure");
                meta.setDlqReason("Stale");
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(System.currentTimeMillis());
                metaRepo.updateversion(meta.getMetaId(), meta);
                return true ;
            }
            if (status >= 400 && status < 500) {
                meta.setEsStatus("failure");
                meta.setDlqReason(reason);
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(null);
                metaRepo.updateversion(meta.getMetaId(), meta);
                return false;
            }
            // transient → DLQ
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", id,
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(id)
                            .version(version)
                            .entity(existing)
                            .build()
            );
            return false;
        } catch (IOException ioe) {
            meta.setEsStatus("failure");
            meta.setDlqReason(ioe.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);
            kafkaTemplate.send("dlq110", id,
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(id)
                            .version(version)
                            .entity(existing)
                            .build()
            );
            return false;
        }
        catch (Exception ex) {
            // transient errors (including your RuntimeException)
            meta.setEsStatus("failure");
            meta.setDlqReason(ex.getMessage());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(meta.getMetaId(), meta);

            kafkaTemplate.send(
                    "dlq110",
                    id,
                    EntityEventVersion.builder()
                            .entityMetadataversion(meta)
                            .entityId(id)
                            .version(version)
                            .entity(existing)
                            .build()
            );
            return false ;
        }
    }
}
