package com.Packages.Repository;

import com.Packages.DTO.EntityDTO;
import com.Packages.Kafka.EntityProducer;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.RepositoryInterface.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.Optional;

public class EntityMongoKafkaRepository {
    EntityMongoRepository entityMongoRepository;
    public EntityMongoKafkaRepository(EntityMongoRepository entityMongoRepository) {
        this.entityMongoRepository = entityMongoRepository;
    }
    @Autowired
    private EntityProducer kafkaProducer;
    public EntityDTO createEntity(EntityDTO entityDTO){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.save(entity);
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEvent.builder()
                .entityDTO(kafkaEvent)
                .operation("create")
                .id(kafkaEvent.getId())
                .build();
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public Optional<Entity> getEntity(String documentId) {
        Optional<Entity> entity =entityMongoRepository.findById(documentId);
        return entity;
    }

    public EntityDTO updateEntity(String DocumentId , EntityDTO entityDTO, LocalDateTime createTime){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(DocumentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.save(entity);
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEvent.builder()
                .entityDTO(kafkaEvent)
                .operation("update")
                .id(kafkaEvent.getId())
                .build();
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public boolean deleteEntity(String documentId){
        if (entityMongoRepository.existsById(documentId)) {
            entityMongoRepository.deleteById(documentId);
            EntityEvent entityEvent = EntityEvent.builder()
                    .operation("delete")
                    .id(documentId)
                    .build();
            return true;
        }
        return false ;
    }
}
