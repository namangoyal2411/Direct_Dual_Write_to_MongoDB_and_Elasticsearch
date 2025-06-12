
package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityMetadata;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class EntityMetadataRepository {
    private final ElasticsearchClient elasticsearchClient;
    public EntityMetadataRepository(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }
    public void save (EntityMetadata entityMetadata){
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
    public Long getLatestOperationSeq(String entityId) {
        try {
            SearchRequest.Builder searchBuilder = new SearchRequest.Builder();
            searchBuilder.index("entity_metadata");
            searchBuilder.query(new Query.Builder()
                    .term(tq -> tq
                            .field("entityId")
                            .value(entityId))
                    .build());
            searchBuilder.sort(new SortOptions.Builder()
                    .field(fq -> fq
                            .field("operationSeq")
                            .order(SortOrder.Desc))
                    .build());
            searchBuilder.size(1);

            SearchResponse<EntityMetadata> response =
                    elasticsearchClient.search(searchBuilder.build(), EntityMetadata.class);
            if (!response.hits().hits().isEmpty()) {
                return response.hits().hits().get(0).source().getOperationSeq();
            } else {
                return 0L;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return 0L;
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
