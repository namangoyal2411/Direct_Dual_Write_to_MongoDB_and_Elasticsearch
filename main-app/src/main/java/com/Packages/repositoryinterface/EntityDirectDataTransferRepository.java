package com.Packages.repositoryinterface;

import com.Packages.model.EntityDirectDataTransfer;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EntityDirectDataTransferRepository
        extends MongoRepository<EntityDirectDataTransfer, String> {
}