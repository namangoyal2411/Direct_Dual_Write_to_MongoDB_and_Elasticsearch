

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
import org.springframework.stereotype.Repository;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
@Repository
public class EntityMetadataRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityMetadataRepository(ElasticsearchClient elasticsearchClient) {
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
    public EntityMetadata findByEntityIdAndOperation(String entityId, String operation) {
        try {
            SearchRequest req = SearchRequest.of(s -> s
                    .index("entity_metadata")
                    .query(q -> q
                            .bool(b -> b
                                    .must(m1 -> m1
                                            .term(t -> t
                                                    .field("entityId")
                                                    .value(entityId)))
                                    .must(m2 -> m2
                                            .term(t -> t
                                                    .field("operation")
                                                    .value(operation)))
                            )
                    )
                    .size(1)
            );

            SearchResponse<EntityMetadata> resp =
                    elasticsearchClient.search(req, EntityMetadata.class);

            if (!resp.hits().hits().isEmpty()) {
                return resp.hits().hits().get(0).source();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public void update(String metaId, EntityMetadata updatedMeta) {
        try {
            UpdateRequest<EntityMetadata, EntityMetadata> request = new UpdateRequest.Builder<EntityMetadata, EntityMetadata>()
                    .index("entity_metadata")
                    .id(metaId)
                    .doc(updatedMeta)
                    .build();
            elasticsearchClient.update(request, EntityMetadata.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}