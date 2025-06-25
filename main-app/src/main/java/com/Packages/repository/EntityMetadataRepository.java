

package com.Packages.repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.model.EntityMetadataversion;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
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
    public void saveversion(EntityMetadataversion entityMetadataversion){
        if(entityMetadataversion == null)
            return ;
        try {
            IndexRequest<EntityMetadataversion> request = IndexRequest.of(i -> i
                    .index("entity_metadata")
                    .id(entityMetadataversion.getMetaId())
                    .document(entityMetadataversion)
            );
            IndexResponse response=  elasticsearchClient.index(request);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public EntityMetadata getById(String metaId) {
        try {
            GetRequest request = new GetRequest.Builder()
                    .index("entity_metadata")
                    .id(metaId)
                    .build();
            GetResponse<EntityMetadata> response = elasticsearchClient.get(request, EntityMetadata.class);
            return response.found() ? response.source() : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public EntityMetadataversion getByIdversion(String metaId) {
        try {
            GetRequest request = new GetRequest.Builder()
                    .index("entity_metadata")
                    .id(metaId)
                    .build();
            GetResponse<EntityMetadataversion> response = elasticsearchClient.get(request, EntityMetadataversion.class);
            return response.found() ? response.source() : null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    public void update(String metaId, EntityMetadata updatedMeta) {
        try {
            UpdateRequest<EntityMetadata, EntityMetadata> request = new UpdateRequest.Builder<EntityMetadata, EntityMetadata>()
                    .index("entity_metadata")
                    .id(metaId)
                    .doc(updatedMeta)
                    .docAsUpsert(true)
                    .build();
            elasticsearchClient.update(request, EntityMetadata.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void updateversion(String metaId, EntityMetadataversion updatedMetaversion) {
        try {
            UpdateRequest<EntityMetadataversion, EntityMetadataversion> request = new UpdateRequest.Builder<EntityMetadataversion, EntityMetadataversion>()
                    .index("entity_metadata")
                    .id(metaId)
                    .doc(updatedMetaversion)
                    .build();
            elasticsearchClient.update(request, EntityMetadataversion.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}