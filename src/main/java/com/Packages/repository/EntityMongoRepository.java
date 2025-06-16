package com.Packages.repository;

import com.Packages.model.Entity;
import com.Packages.repositoryinterface.MongoRepositoryInterface;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;        // Spring‐Data’s Query
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityMongoRepository {
    private final MongoRepositoryInterface mongoRepositoryInterface;
    private final MongoOperations       mongoOps;

    @Autowired
    public EntityMongoRepository(MongoRepositoryInterface mongoRepositoryInterface,
                                 MongoOperations mongoOps) {
        this.mongoRepositoryInterface = mongoRepositoryInterface;
        this.mongoOps                = mongoOps;
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
    public long nextSequence(String entityId) {
        Query q = Query.query(Criteria.where("_id").is(entityId));
        Update u = new Update().inc("seq", 1);
        FindAndModifyOptions opts = FindAndModifyOptions.options()
                .upsert(true)
                .returnNew(true);

        Document result = mongoOps.findAndModify(
                q, u, opts,
                Document.class,
                "entity_op_seq"
        );
        if (result != null && result.containsKey("seq")) {
            Number seqNum = result.get("seq", Number.class);
            return (seqNum != null) ? seqNum.longValue() : 1L;
        }
        return 1L;
    }
}
