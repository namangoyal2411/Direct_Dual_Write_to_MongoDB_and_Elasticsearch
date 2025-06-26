package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.kafka.EntityProducer;
import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class KafkaSyncService {
    private static final String ES_INDEX = "entity";
    private final EntityMongoRepository mongoRepo;
    private final EntityMetadataRepository metaRepo;
    private final EntityProducer kafkaProducer;

    public KafkaSyncService(EntityMongoRepository mongoRepo,
                            EntityMetadataRepository metaRepo,
                            EntityProducer kafkaProducer) {
        this.mongoRepo = mongoRepo;
        this.metaRepo = metaRepo;
        this.kafkaProducer = kafkaProducer;
    }

    public EntityDTO createEntity(EntityDTO dto) {
        long mongoTs = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.now();
        Entity e = Entity.builder()
                .id(dto.getId())
                .name(dto.getName())
                .createTime(now)
                .modifiedTime(now)
                .build();
        mongoRepo.createEntity(e);
        dto.setId(e.getId());
        EntityMetadata meta = buildMetadata(e.getId(), "create", e.getVersion(), mongoTs);
        metaRepo.save(meta);
        kafkaProducer.sendToKafka(
                buildEvent("create", e, e.getId(), ES_INDEX, meta)
        );
        return dto;
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        long mongoTs = System.currentTimeMillis();
        LocalDateTime now = LocalDateTime.now();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        e.setName(dto.getName());
        e.setModifiedTime(now);
        mongoRepo.updateEntity(e);
        dto.setId(e.getId());
        EntityMetadata meta = buildMetadata(id, "update", e.getVersion(), mongoTs);
        metaRepo.save(meta);
        kafkaProducer.sendToKafka(
                buildEvent("update", e, id, ES_INDEX, meta)
        );
        return dto;
    }

    public boolean deleteEntity(String id) {
        long mongoTs = System.currentTimeMillis();
        Entity e = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        boolean deleted = mongoRepo.deleteEntity(id);
        if (!deleted) {
            return false;
        }
        EntityMetadata meta = buildMetadata(id, "delete", e.getVersion() + 1, mongoTs);
        metaRepo.save(meta);
        kafkaProducer.sendToKafka(
                buildEvent("delete", Entity.builder().id(id).build(), id, ES_INDEX, meta)
        );
        return true;
    }

    private EntityMetadata buildMetadata(String entityId,
                                         String operation,
                                         long operationSeq,
                                         long mongoWriteMillis) {
        return EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entityId)
                .approach("Kafka Sync")
                .operation(operation)
                .operationSeq(operationSeq)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("success")
                .esStatus("pending")
                .dlqReason(null)
                .build();
    }

    private EntityEvent buildEvent(String operation,
                                   Entity entity,
                                   String documentId,
                                   String indexName,
                                   EntityMetadata metadata) {
        return EntityEvent.builder()
                .entity(entity)
                .operation(operation)
                .id(documentId)
                .index(indexName)
                .entityMetadata(metadata)
                .build();
    }
}

//package com.Packages.service;
//
//import com.Packages.dto.EntityDTO;
//import com.Packages.exception.EntityNotFoundException;
//import com.Packages.kafka.EntityProducer;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityEvent;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repository.EntityMetadataRepository;
//import com.Packages.repository.EntityMongoRepository;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Service
//public class KafkaSyncService {
//    EntityMongoRepository entityMongoRepository;
//    EntityMetadataRepository entityMetadataRepository;
//    @Autowired
//    public KafkaSyncService(EntityMongoRepository entityMongoRepository,EntityMetadataRepository entityMetadataRepository) {
//        this.entityMongoRepository = entityMongoRepository;
//        this.entityMetadataRepository= entityMetadataRepository;
//    }
//    @Autowired
//    private EntityProducer kafkaProducer;
//    public EntityDTO createEntity(EntityDTO entityDTO){
//        long mongoWriteMillis = System.currentTimeMillis();
//        String indexName="entity";
//        LocalDateTime localDateTime= LocalDateTime.now();
//        Entity entity = new Entity(
//                entityDTO.getId(),
//                entityDTO.getName(),
//                localDateTime,
//                localDateTime,
//                null
//        );
//        entityMongoRepository.createEntity(entity);
//        long operationSeq = entity.getVersion();
//        entityDTO.setId(entity.getId());
//        EntityMetadata entityMetadata = EntityMetadata.builder()
//                .metaId(UUID.randomUUID().toString())
//                .entityId(entity.getId())
//                .approach("Kafka Sync")
//                .operation("create")
//                .operationSeq(operationSeq)
//                .mongoWriteMillis(mongoWriteMillis)
//                .esSyncMillis(null)
//                .syncAttempt(0)
//                .mongoStatus("success")
//                .esStatus("pending")
//                .dlqReason(null)
//                .build();
//        entityMetadataRepository.save(entityMetadata);
//        EntityEvent entityEvent = EntityEventmapper("create",entity, entity.getId(), indexName,entityMetadata);
//        kafkaProducer.sendToKafka(entityEvent);
//
//        return entityDTO;
//    }
//    public EntityDTO updateEntity(String documentId,EntityDTO entityDTO){
//        long mongoWriteMillis = System.currentTimeMillis();
//        LocalDateTime now = LocalDateTime.now();
//        String indexName="entity";
//        Entity entity = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        entity.setName(entityDTO.getName());
//        entity.setModifiedTime(now);
//        entity = entityMongoRepository.updateEntity(entity);
//        long operationSeq = entity.getVersion();
//        EntityMetadata entityMetadata = EntityMetadata.builder()
//                .metaId(UUID.randomUUID().toString())
//                .entityId(documentId)
//                .approach("Kafka Sync")
//                .operation("update")
//                .operationSeq(operationSeq)
//                .mongoWriteMillis(mongoWriteMillis)
//                .esSyncMillis(null)
//                .syncAttempt(0)
//                .mongoStatus("success")
//                .esStatus("pending")
//                .dlqReason(null)
//                .build();
//        entityMetadataRepository.save(entityMetadata);
//        EntityEvent entityEvent = EntityEventmapper("update",entity, entity.getId(), indexName,entityMetadata);
//        kafkaProducer.sendToKafka(entityEvent);
//        return entityDTO;
//    }
//    public boolean deleteEntity(String documentId ){
//        long mongoWriteMillis = System.currentTimeMillis();
//        String indexName="entity";
//        Entity e = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        long operationSeq = e.getVersion()+1;
//        boolean proceed=entityMongoRepository.deleteEntity(documentId);
//        if (proceed) {
//            Entity entity = Entity.builder().id(documentId).build();
//            EntityMetadata entityMetadata = EntityMetadata.builder()
//                    .metaId(UUID.randomUUID().toString())
//                    .entityId(documentId)
//                    .approach("Kafka Sync")
//                    .operation("delete")
//                    .operationSeq(operationSeq)
//                    .mongoWriteMillis(mongoWriteMillis)
//                    .esSyncMillis(null)
//                    .syncAttempt(0)
//                    .mongoStatus("success")
//                    .esStatus("pending")
//                    .dlqReason(null)
//                    .build();
//            entityMetadataRepository.save(entityMetadata);
//            EntityEvent entityEvent = EntityEventmapper("delete",entity, entity.getId(), indexName,entityMetadata);
//            kafkaProducer.sendToKafka(entityEvent);
//            return true;
//        }
//        return false ;
//    }
//    public EntityEvent EntityEventmapper(String operation,Entity entity, String documentId, String indexName,EntityMetadata metadata){
//        EntityEvent entityEvent= EntityEvent.builder()
//                .entity(entity)
//                .operation(operation)
//                .id(documentId)
//                .index(indexName)
//                .entityMetadata(metadata)
//                .build();
//        return entityEvent;
//    }
//}