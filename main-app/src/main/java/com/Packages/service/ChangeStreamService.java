package com.Packages.service;

import com.Packages.dto.EntityDTO;
import com.Packages.exception.EntityNotFoundException;
import com.Packages.model.Entity;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class ChangeStreamService {

    private final EntityMongoRepository mongoRepo;

    public ChangeStreamService(EntityMongoRepository mongoRepo) {
        this.mongoRepo = mongoRepo;
    }

    public EntityDTO createEntity(EntityDTO dto) {
        LocalDateTime now = LocalDateTime.now();
        Entity entity = Entity.builder().id(dto.getId()).name(dto.getName()).createTime(now).modifiedTime(now).build();
        mongoRepo.createEntity(entity);
        dto.setId(entity.getId());
        return dto;
    }

    public EntityDTO updateEntity(String id, EntityDTO dto) {
        Entity entity = mongoRepo.getEntity(id).orElseThrow(() -> new EntityNotFoundException(id));
        entity.setName(dto.getName());
        entity.setModifiedTime(LocalDateTime.now());
        mongoRepo.updateEntity(entity);
        dto.setId(entity.getId());
        return dto;
    }

    public boolean deleteEntity(String id) {
        mongoRepo.getEntity(id).orElseThrow(() -> new EntityNotFoundException(id));
        return mongoRepo.deleteEntity(id);
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
