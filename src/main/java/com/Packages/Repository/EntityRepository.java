package com.Packages.Repository;

import com.Packages.DTO.EntityDTO;
import com.Packages.Kafka.EntityProducer;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.RepositoryInterface.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public class EntityRepository {
    EntityMongoRepository entityMongoRepository;
    public EntityRepository(EntityMongoRepository entityMongoRepository) {
        this.entityMongoRepository = entityMongoRepository;
    }
    @Autowired
    private EntityProducer kafkaProducer;
    public Entity createEntity(Entity entity){
        entityMongoRepository.save(entity);
        return entity;
    }
    public Optional<Entity> getEntity(String documentId) {
        Optional<Entity> entity =entityMongoRepository.findById(documentId);
        return entity;
    }

    public Entity updateEntity(Entity entity){
        entityMongoRepository.save(entity);
        return entity;
    }
    public boolean deleteEntity(String documentId){
        if (entityMongoRepository.existsById(documentId)) {
            entityMongoRepository.deleteById(documentId);
            return true;
        }
        return false ;
    }
}
