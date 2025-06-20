package com.Packages.repositoryinterface;

import com.Packages.model.ChangeStreamState;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository

public interface ChangeStreamStateRepository
        extends MongoRepository<ChangeStreamState, String> {
}