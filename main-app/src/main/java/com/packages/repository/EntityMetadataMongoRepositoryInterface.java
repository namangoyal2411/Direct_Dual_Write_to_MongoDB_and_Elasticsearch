package com.packages.repository;

import com.packages.model.EntityMetadata;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface EntityMetadataMongoRepositoryInterface extends MongoRepository<EntityMetadata,String > {

}