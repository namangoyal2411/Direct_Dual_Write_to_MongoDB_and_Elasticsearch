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

//package com.Packages.service;
//
//import com.Packages.dto.EntityDTO;
//import com.Packages.exception.EntityNotFoundException;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repository.EntityMongoRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.model.Filters;
//import com.mongodb.client.model.Updates;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Service
//public class ChangeStreamService {
//    private final EntityMongoRepository entityMongoRepository;
//    private final EntityMetadataRepository entityMetadataRepository;
//    private final MongoClient mongoClient;
//    @Autowired
//    public ChangeStreamService(EntityMongoRepository entityMongoRepository,
//                               EntityMetadataRepository entityMetadataRepository,MongoClient mongoClient) {
//        this.entityMongoRepository = entityMongoRepository;
//        this.entityMetadataRepository = entityMetadataRepository;
//        this.mongoClient = mongoClient;
//    }
//    public EntityDTO createEntity(EntityDTO entityDTO) {
//        long mongoWriteMillis = System.currentTimeMillis();
//        String indexName = "entity";
//
//        LocalDateTime localDateTime = LocalDateTime.now();
//        Entity entity = Entity.builder()
//                .id(entityDTO.getId())
//                .name(entityDTO.getName())
//                .createTime(localDateTime)
//                .modifiedTime(localDateTime)
//                .build();
//        entityMongoRepository.createEntity(entity);
//        return entityDTO;
//    }
//
//    public EntityDTO updateEntity(String documentId, EntityDTO entityDTO) {
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity existing = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        existing.setName(entityDTO.getName());
//        existing.setModifiedTime(LocalDateTime.now());
//        Entity updatedEntity = entityMongoRepository.updateEntity(existing);
//        updatedEntity.getVersion();
//        return entityDTO;
//    }
//    public boolean deleteEntity(String documentId) {
//        long mongoWriteMillis = System.currentTimeMillis();
//        Entity existing = entityMongoRepository.getEntity(documentId)
//                .orElseThrow(() -> new EntityNotFoundException(documentId));
//        boolean mongoDeleted = entityMongoRepository.deleteEntity(documentId);
//        return mongoDeleted;
//    }
//}
