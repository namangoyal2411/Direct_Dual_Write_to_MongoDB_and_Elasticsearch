package com.packages.repository;

import com.packages.model.ChangeStreamState;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository

public interface ChangeStreamStateRepository
        extends MongoRepository<ChangeStreamState, String> {
}