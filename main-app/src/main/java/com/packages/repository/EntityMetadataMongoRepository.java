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

    public Optional<EntityMetadata> findById(String metaId) {
        return repo.findById(metaId);
    }

    public boolean existsById(String metaId) {
        return repo.existsById(metaId);
    }

    public void deleteById(String metaId) {
        repo.deleteById(metaId);
    }
}
