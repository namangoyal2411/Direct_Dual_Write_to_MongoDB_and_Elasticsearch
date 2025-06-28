

package com.packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import com.packages.model.EntityMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Repository
public class EntityMetadataRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityMetadataRepository( @Qualifier("metadataClient") ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }
    public void save(EntityMetadata entityMetadata){
        if(entityMetadata == null)
            return ;
        try {
            IndexRequest<EntityMetadata> request = IndexRequest.of(i -> i
                    .index("entity_metadata")
                    .id(entityMetadata.getMetaId())
                    .document(entityMetadata)
            );
            IndexResponse response=  elasticsearchClient.index(request);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}