package com.approach_1.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import com.approach_1.DTO.EntityDTO;
import com.approach_1.Model.Entity;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public class EntityElasticRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityElasticRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public EntityDTO createEntity(String indexName , EntityDTO entityDTO){
        System.out.println("Attempting to write to ES: " + indexName + ", id: " + entityDTO.getId());

        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(localDateTime).
                modifiedTime(localDateTime).
                build();
      try {
          IndexRequest<Entity> request =IndexRequest.of (i->i
                                        .index(indexName)
                                        .id(entity.getId())
                                        .document(entity)
          );
      IndexResponse response = elasticsearchClient.index(request);
      }
      catch (Exception e) {
          e.printStackTrace();
      }
      return entityDTO;
    }
    public Entity getEntity(String indexName,String documentId) {
        try {
            GetRequest getRequest = GetRequest.of(g -> g.index(indexName).id(documentId));
            GetResponse<Entity> response = elasticsearchClient.get(getRequest, Entity.class);
            if (response.found()) {
                Entity entity = response.source();
                return entity;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public EntityDTO updateEntity(String indexName, String documentId,EntityDTO entityDTO,LocalDateTime createTime){
        LocalDateTime localDateTime= LocalDateTime.now();
        Entity entity = Entity.builder().
                id(entityDTO.getId()).
                name(entityDTO.getName()).
                createTime(createTime).
                modifiedTime(localDateTime).
                build();
        try {
            UpdateRequest<Entity,Entity> request =UpdateRequest.of (u->u
                    .index(indexName)
                    .id(documentId)
                    .doc(entity).
                    docAsUpsert(true)
            );
            UpdateResponse<Entity> response = elasticsearchClient.update(request,Entity.class);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return entityDTO;
    }
    public boolean deleteEntity(String indexName, String documentId){
        try {
            elasticsearchClient.delete(d->d.index(indexName).id(documentId));
            return true;
        }
        catch (Exception e){
            e.printStackTrace();
            return false ;
        }
    }

}
