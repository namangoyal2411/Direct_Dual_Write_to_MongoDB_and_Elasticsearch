package com.Packages.Service;

import com.Packages.DTO.EntityDTO;
import com.Packages.Exception.EntityNotFoundException;
import com.Packages.Kafka.EntityProducer;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Model.EntityMetadata;
import com.Packages.Repository.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;

@Service
public class MongoKafkaElasticService {
    EntityMongoRepository entityMongoRepository;
    @Autowired
    public MongoKafkaElasticService(EntityMongoRepository entityMongoRepository) {
        this.entityMongoRepository = entityMongoRepository;
    }
    @Autowired
    private EntityProducer kafkaProducer;
    public EntityDTO createEntity(EntityDTO entityDTO,String indexName){
        LocalDateTime localDateTime= LocalDateTime.now();
        long mongoWriteMillis = System.currentTimeMillis();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.createEntity(entity);
        EntityMetadata entityMetadata = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(entity.getId())
                .operation("create")
                .operationSeq(1L)
                .mongoWriteMillis(mongoWriteMillis)
                .esSyncMillis(null)
                .syncAttempt(0)
                .mongoStatus("Success")
                .esStatus("PENDING")
                .dlqReason(null)
                .build();
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEvent.builder()
                .entityDTO(kafkaEvent)
                .operation("create")
                .id(kafkaEvent.getId())
                .index(indexName)
                .metadata(entityMetadata)
                .build();
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public EntityDTO updateEntity(String indexName,String documentId,EntityDTO entityDTO){
        Entity mongoEntity = entityMongoRepository
                .getEntity(documentId)
                .orElseThrow(() -> new EntityNotFoundException(documentId));
        LocalDateTime createTime = mongoEntity.getCreateTime();
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(documentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.updateEntity(entity);
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEvent.builder()
                .entityDTO(kafkaEvent)
                .operation("update")
                .id(kafkaEvent.getId())
                .index(indexName)
                .build();
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public boolean deleteEntity(String indexName,String documentId ){
        if (entityMongoRepository.deleteEntity(documentId)) {
            EntityDTO entityDTO = EntityDTO.builder().id(documentId).build();
            EntityEvent entityEvent = EntityEvent.builder()
                    .entityDTO(entityDTO)
                    .operation("delete")
                    .id(documentId)
                    .index(indexName)
                    .build();
            kafkaProducer.sendToKafka(entityEvent);
            return true;
        }
        return false ;
    }
}
