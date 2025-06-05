package com.approach_1.Service;

import com.approach_1.DTO.EntityDTO;
import com.approach_1.Model.Entity;
import com.approach_1.Repository.EntityElasticRepository;
import com.approach_1.Repository.EntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class EntityService {
    EntityRepository entityRepository;
    EntityElasticRepository entityElasticRepository;
    @Autowired
    public EntityService(EntityRepository entityRepository, EntityElasticRepository entityElasticRepository) {
        this.entityRepository = entityRepository;
        this.entityElasticRepository = entityElasticRepository;
    }

    public EntityDTO createEntity(EntityDTO entityDTO,String indexName){
        entityRepository.createEntity(entityDTO);
        entityElasticRepository.createEntity(indexName,entityDTO);
        return entityDTO;
    }
    public EntityDTO updateEntity(String indexName,String documentId,EntityDTO entityDTO){
        Optional<Entity> mongoEntityOpt = entityRepository.getEntity(documentId);
        LocalDateTime createTime;

        if (mongoEntityOpt.isPresent()) {
            createTime = mongoEntityOpt.get().getCreateTime();
        } else {
            LocalDateTime esCreateTime = entityElasticRepository.getEntity(indexName, documentId).getCreateTime();
            if (esCreateTime != null) {
                createTime = esCreateTime;
            } else {
                createTime = LocalDateTime.now();
            }
        }

        entityRepository.updateEntity(documentId , entityDTO,createTime);
        entityElasticRepository.updateEntity(indexName,documentId,entityDTO,createTime);
        return entityDTO;
    }
    public boolean deleteEntity(String indexName,String documentId ){
        if (entityRepository.deleteEntity(documentId)&&entityElasticRepository.deleteEntity(indexName,documentId))
            return true ;
        return false ;
    }
}
