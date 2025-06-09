package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
public class EntityElasticRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityElasticRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    public Entity createEntity(String indexName , Entity entity){
      try {
          IndexRequest<Entity> request =IndexRequest.of (i->i
                                        .index(indexName)
                                        .id(entity.getId())
                                        .document(entity)
          );
          System.out.println("naman");
      IndexResponse response = elasticsearchClient.index(request);
      }
      catch (Exception e) {
          e.printStackTrace();
      }
      return entity;
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
    public Entity updateEntity(String indexName, String documentId,Entity entity,LocalDateTime createTime){
        try {
            UpdateRequest<Entity,Entity> request =UpdateRequest.of (u->u
                    .index(indexName)
                    .id(documentId)
                    .doc(entity)
                    .docAsUpsert(true)
            );
            UpdateResponse<Entity> response = elasticsearchClient.update(request,Entity.class);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return entity;
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
