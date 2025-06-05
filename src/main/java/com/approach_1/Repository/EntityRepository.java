package com.approach_1.Repository;

import com.approach_1.DTO.EntityDTO;
import com.approach_1.Model.Entity;
import com.approach_1.RepositoryInterface.EntityMongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public class EntityRepository {
    EntityMongoRepository entityMongoRepository;
    public EntityRepository(EntityMongoRepository entityMongoRepository) {
        this.entityMongoRepository = entityMongoRepository;
    }

    public EntityDTO createEntity(EntityDTO entityDTO){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                        id(entityDTO.getId()).
                        name(entityDTO.getName()).
                        createTime(localDateTime).
                        modifiedTime(localDateTime).
                        build();
        entityMongoRepository.save(entity);

        return entityDTO;
    }
    public Optional<Entity> getEntity(String documentId) {
        Optional<Entity> entity =entityMongoRepository.findById(documentId);
        return entity;
    }

    public EntityDTO updateEntity(String DocumentId , EntityDTO entityDTO, LocalDateTime createTime){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(DocumentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityMongoRepository.save(entity);
        return entityDTO;
    }
    public boolean deleteEntity(String DocumentId){
        if (entityMongoRepository.existsById(DocumentId)) {
            entityMongoRepository.deleteById(DocumentId);
            return true;
        }
        return false ;
    }

}
