package com.approach_1.RepositoryInterface;

import com.approach_1.Model.Entity;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface  EntityMongoRepository extends MongoRepository<Entity, String > {

}
