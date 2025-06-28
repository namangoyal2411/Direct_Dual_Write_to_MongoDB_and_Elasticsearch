package com.packages.repository;

import com.packages.model.Entity;
import com.packages.repository.MongoRepositoryInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityMongoRepository {
    private final MongoRepositoryInterface mongoRepositoryInterface;
    private final MongoOperations mongoOps;

    @Autowired
    public EntityMongoRepository(MongoRepositoryInterface mongoRepositoryInterface,
                                 MongoOperations mongoOps) {
        this.mongoRepositoryInterface = mongoRepositoryInterface;
        this.mongoOps = mongoOps;
    }

    public Entity createEntity(Entity entity) {
        return mongoRepositoryInterface.save(entity);
    }

    public Optional<Entity> getEntity(String documentId) {
        return mongoRepositoryInterface.findById(documentId);
    }

    public Entity updateEntity(Entity entity) {
        return mongoRepositoryInterface.save(entity);
    }

    public boolean deleteEntity(String documentId) {
        if (mongoRepositoryInterface.existsById(documentId)) {
            mongoRepositoryInterface.deleteById(documentId);
            return true;
        }
        return false;
    }
}
