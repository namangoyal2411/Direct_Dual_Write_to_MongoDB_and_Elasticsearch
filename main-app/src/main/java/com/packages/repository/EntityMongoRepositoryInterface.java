package com.packages.repository;

import com.packages.model.Entity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EntityMongoRepositoryInterface extends MongoRepository<Entity, String> {

}