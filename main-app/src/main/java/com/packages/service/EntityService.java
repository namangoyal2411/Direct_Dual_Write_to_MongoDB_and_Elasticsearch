package com.packages.service;
import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.repository.EntityMongoRepository;
import com.packages.util.EntityUtil;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class EntityService {

    private final EntityMongoRepository mongoRepo;

    public EntityService(EntityMongoRepository mongoRepo) {
        this.mongoRepo = mongoRepo;
    }

    public Entity createEntity(Entity ent) {
        LocalDateTime now = LocalDateTime.now();
        Entity toSave = new Entity(null, ent.getName(), now, now, false ,null);
        Entity saved = mongoRepo.createEntity(toSave);
        return saved;
    }

    public Entity updateEntity(String id, Entity ent) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity updated = EntityUtil.updateEntity(ent, existing);
       updated  = mongoRepo.updateEntity(updated);
        return updated;
    }

    public boolean deleteEntity(String id) {
        Entity existing = mongoRepo.getEntity(id)
                .orElseThrow(() -> new EntityNotFoundException(id));
        Entity toUpdate = EntityUtil.markDeleted(existing);
       mongoRepo.updateEntity(toUpdate);
        return true;
    }
}

