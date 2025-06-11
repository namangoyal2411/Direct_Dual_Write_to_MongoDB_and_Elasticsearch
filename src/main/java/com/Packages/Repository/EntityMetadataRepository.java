package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
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
    public EntityMetadata getEntityMetaData(String entityId, String operation, long operationSeq) {
        try {
            SearchResponse<EntityMetadata> response = elasticsearchClient.search(s -> s
                            .index("entity_metadata")
                            .query(q -> q
                                    .bool(b -> b
                                            .must(
                                                    m -> m.term(t -> t.field("entityId").value(entityId)),
                                                    m -> m.term(t -> t.field("operation").value(operation)),
                                                    m -> m.term(t -> t.field("operationSeq").value(operationSeq))
                                            )
                                    )
                            )
                            .sort(s -> s
                                    .field(f -> f.field("syncAttempt").order(SortOrder.Desc))
                            )
                            .size(1),
                    EntityMetadata.class
            );

            return response.hits().hits().isEmpty() ? null : response.hits().hits().get(0).source();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Long getLatestOperationSeq(String entityId) {
        try {
            SearchResponse<EntityMetadata> response = elasticsearchClient.search(s -> s
                            .index("entity_metadata")
                            .query(q -> q
                                    .bool(b -> b
                                            .must(
                                                    m -> m.term(t -> t.field("entityId").value(entityId)),
                                            )
                                    )
                            )
                            .sort(s -> s
                                    .field(f -> f.field("operationSeq").order(SortOrder.Desc))
                            )
                            .size(1),
                    EntityMetadata.class
            );

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


}

