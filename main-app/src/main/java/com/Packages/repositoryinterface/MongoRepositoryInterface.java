package com.Packages.repositoryinterface;

import com.Packages.model.Entity;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MongoRepositoryInterface extends MongoRepository<Entity, String> {

}
