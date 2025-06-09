package com.Packages.RepositoryInterface;

import com.Packages.Model.Entity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface  EntityMongoRepository extends MongoRepository<Entity, String > {

}
