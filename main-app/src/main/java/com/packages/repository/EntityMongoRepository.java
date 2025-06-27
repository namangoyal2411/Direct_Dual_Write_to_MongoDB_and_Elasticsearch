package com.packages.repository;

import com.packages.model.Entity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityMongoRepository {
    private final EntityMongoRepositoryInterface entityMongoRepositoryInterface;

    @Autowired
    public EntityMongoRepository(EntityMongoRepositoryInterface entityMongoRepositoryInterface) {
        this.entityMongoRepositoryInterface = entityMongoRepositoryInterface;
    }

    public Entity createEntity(Entity entity) {
        return entityMongoRepositoryInterface.save(entity);
    }

    public Optional<Entity> getEntity(String documentId) {
        return entityMongoRepositoryInterface.findById(documentId);
    }

    public Entity updateEntity(Entity entity) {
        return entityMongoRepositoryInterface.save(entity);
    }

    public boolean deleteEntity(String documentId) {
        if (entityMongoRepositoryInterface.existsById(documentId)) {
            entityMongoRepositoryInterface.deleteById(documentId);
            return true;
        }
        return false;
    }
}