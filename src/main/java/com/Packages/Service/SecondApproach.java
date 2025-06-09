package com.Packages.Service;

import com.Packages.DTO.EntityDTO;
import com.Packages.Exception.EntityNotFoundException;
import com.Packages.Kafka.EntityProducer;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Repository.EntityMongoKafkaRepository;
import com.Packages.Repository.EntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class SecondApproach {
    EntityRepository entityRepository;
    @Autowired
    public SecondApproach(EntityRepository entityRepository) {
        this.entityRepository = entityRepository;
    }
    @Autowired
    private EntityProducer kafkaProducer;
    public EntityDTO createEntity(EntityDTO entityDTO,String indexName){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        entityRepository.createEntity(entity);
        EntityDTO kafkaEvent = EntityDTO.fromEntity(entity);
        EntityEvent entityEvent = EntityEvent.builder()
                .entityDTO(kafkaEvent)
                .operation("create")
                .id(kafkaEvent.getId())
                .index(indexName)
                .build();
        kafkaProducer.sendToKafka(entityEvent);
        return entityDTO;
    }
    public EntityDTO updateEntity(String indexName,String documentId,EntityDTO entityDTO){
        Entity mongoEntity = entityRepository
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
        entityRepository.updateEntity(entity);
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
        if (entityRepository.deleteEntity(documentId)) {
            EntityEvent entityEvent = EntityEvent.builder()
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
