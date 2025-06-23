package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEventVersion;
import com.Packages.model.EntityMetadataversion;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class KafkaSyncServiceVersion {
    private final EntityMongoRepository                   mongoRepo;
    private final EntityMetadataRepository                metaRepo;
    private final KafkaTemplate<String, EntityEventVersion> kafkaVersionTemplate;

    @Autowired
    public KafkaSyncServiceVersion(
            EntityMongoRepository mongoRepo,
            EntityMetadataRepository metaRepo,
            @Qualifier("entityEventVersionKafkaTemplate")
            KafkaTemplate<String, EntityEventVersion> kafkaVersionTemplate
    ) {
        this.mongoRepo            = mongoRepo;
        this.metaRepo             = metaRepo;
        this.kafkaVersionTemplate = kafkaVersionTemplate;
    }
    private static final Logger log = LoggerFactory.getLogger(KafkaSyncServiceVersion.class);
    public EntityDTO createEntity(EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = Entity.builder()
                .id(dto.getId())
                .name(dto.getName())
                .createTime(now)
                .modifiedTime(now)
                .build();
        Entity saved = mongoRepo.createEntity(toSave);

        // 2) Record metadata (status pending)
        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(saved.getId())
                .approach("Kafka Sync")
                .operation("create")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);

        // 3) Build and send Kafka event with the version from Mongo
        EntityEventVersion evt = EntityEventVersion.builder()
                .entityMetadataversion(meta)
                .entityId(saved.getId())
                .version(saved.getVersion())
                .entity(saved)
                .build();
        kafkaVersionTemplate.send("entity109", meta.getMetaId(), evt);

        dto.setId(saved.getId());
        return dto;
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        log.info("→ [Service] updateEntity(id={}, dto={})", id, dto);
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        existing.setName(dto.getName());
        existing.setModifiedTime(LocalDateTime.now());
        Entity saved = mongoRepo.updateEntity(existing);

        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Kafka Sync")
                .operation("update")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);

        // 3) Emit update event with the new version
        EntityEventVersion evt = EntityEventVersion.builder()
                .entityMetadataversion(meta)
                .entityId(id)
                .version(saved.getVersion())
                .entity(saved)
                .build();

        kafkaVersionTemplate.send("entity109", meta.getMetaId(), evt);

        dto.setId(id);
        return dto;
    }

    public boolean deleteEntity(String id) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        long version = existing.getVersion()+1;

        boolean removed = mongoRepo.deleteEntity(id);
        if (!removed) {
            return false;
        }

        EntityMetadataversion meta = EntityMetadataversion.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(id)
                .approach("Kafka Sync")
                .operation("delete")
                .mongoWriteMillis(System.currentTimeMillis())
                .esStatus("pending")
                .syncAttempt(0)
                .mongoStatus("success")
                .build();
        metaRepo.saveversion(meta);

        // 4) Emit delete event with that version
        EntityEventVersion evt = EntityEventVersion.builder()
                .entityMetadataversion(meta)
                .entityId(id)
                .version(version)
                .entity(existing)  // carry the last snapshot
                .build();
        kafkaVersionTemplate.send("entity109", meta.getMetaId(), evt);

        return true;
    }
}
