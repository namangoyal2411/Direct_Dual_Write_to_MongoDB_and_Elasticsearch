package com.Packages.Service;

import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import com.Packages.Repository.EntityElasticRepository;
import com.Packages.Repository.EntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;


@Service
public class FirstApproachService {
    EntityRepository entityRepository;
    EntityElasticRepository entityElasticRepository;
    @Autowired
    public FirstApproachService(EntityRepository entityRepository, EntityElasticRepository entityElasticRepository) {
        this.entityRepository = entityRepository;
        this.entityElasticRepository = entityElasticRepository;
    }

    public EntityDTO createEntity(EntityDTO entityDTO,String indexName){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
        entityRepository.createEntity(entity);
        entityElasticRepository.createEntity(indexName,entity);
        return entityDTO;
    }
    public EntityDTO updateEntity(String indexName,String documentId,EntityDTO entityDTO){
        Optional<Entity> mongoEntityOpt = entityRepository.getEntity(documentId);
        LocalDateTime createTime;
        createTime = mongoEntityOpt.get().getCreateTime();
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(documentId).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        entityRepository.updateEntity(entity);
        entityElasticRepository.updateEntity(indexName,documentId,entity,createTime);
        return entityDTO;
    }
    public boolean deleteEntity(String indexName,String documentId ){
        if (entityRepository.deleteEntity(documentId)&&entityElasticRepository.deleteEntity(indexName,documentId))
            return true ;
        return false ;
    }
}
