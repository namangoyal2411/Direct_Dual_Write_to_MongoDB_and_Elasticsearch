package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityMetadata;
import org.springframework.stereotype.Repository;

@Repository
public class EntityMetadataRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityMetadataRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }
    public void save (EntityMetadata entityMetadata){
        try {
            IndexRequest<EntityMetadata> request = IndexRequest.of(i -> i
                    .index("entity_metadata")
                    .id(entityMetadata.getEntityId())
                    .document(entityMetadata)
            );
            IndexResponse response=  elasticsearchClient.index(request);
        }

        catch (Exception e){
            e.printStackTrace();
        }
    }
    public EntityMetadata getEntityMetaData(String documentId) {
        try {
            GetRequest getRequest = GetRequest.of(g -> g.index("entity_metadata").id(documentId));
            GetResponse<EntityMetadata> response = elasticsearchClient.get(getRequest, EntityMetadata.class);
            if (response.found()) {
                EntityMetadata entityMetadata = response.source();
                return entityMetadata;
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

