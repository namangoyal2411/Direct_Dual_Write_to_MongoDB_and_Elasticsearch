package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
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
    public EntityMetadata getEntityMetaData(String documentId) {
        try {
            SearchResponse<EntityMetadata> response = elasticsearchClient.search(s -> s
                            .index("entity_metadata")
                            .query(q -> q
                                    .term(t -> t
                                            .field("entityId")
                                            .value(documentId)
                                    )
                            )
                            .sort(srt -> srt
                                    .field(f -> f
                                            .field("mongoWriteMillis")
                                            .order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)
                                    )
                            )
                            .size(1),
                    EntityMetadata.class
            );
            List<Hit<EntityMetadata>> hits = response.hits().hits();
            if (!hits.isEmpty()) {
                return hits.get(0).source();
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

