package com.Packages.Repository;

import com.Packages.Kafka.EntityProducer;
import com.Packages.Model.Entity;
import com.Packages.RepositoryInterface.MongoRepositoryInterface;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityMongoRepository {
    MongoRepositoryInterface mongoRepositoryInterface;

    public EntityMongoRepository(MongoRepositoryInterface mongoRepositoryInterface) {
        this.mongoRepositoryInterface = mongoRepositoryInterface;
    }

    @Autowired
    private EntityProducer kafkaProducer;

    public Entity createEntity(Entity entity) {

        mongoRepositoryInterface.save(entity);
        return entity;
    }

    public Optional<Entity> getEntity(String documentId) {
        Optional<Entity> entity = mongoRepositoryInterface.findById(documentId);
        return entity;
    }

    public Entity updateEntity(Entity entity) {
        mongoRepositoryInterface.save(entity);
        return entity;
    }

    public boolean deleteEntity(String documentId) {
        if (mongoRepositoryInterface.existsById(documentId)) {
            mongoRepositoryInterface.deleteById(documentId);
            return true;
        }
        return false;
    }
}
