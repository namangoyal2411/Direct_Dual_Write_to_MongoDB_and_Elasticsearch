package com.packages.repository;

import com.packages.model.EntityMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class EntityMetadataMongoRepository {

    private final EntityMetadataMongoRepositoryInterface repo;

    @Autowired
    public EntityMetadataMongoRepository(
            EntityMetadataMongoRepositoryInterface repo
    ) {
        this.repo = repo;
    }

    public EntityMetadata save(EntityMetadata meta) {
        return repo.save(meta);
    }

}
