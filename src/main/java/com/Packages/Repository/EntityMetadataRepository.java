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
    public EntityMetadata getEntityMetaData(String entityId, String operation, long operationSeq) {
        try {
            SearchRequest.Builder requestBuilder = new SearchRequest.Builder();
            requestBuilder.index("entity_metadata");
            requestBuilder.query(q -> q.bool(b -> b
                    .must(List.of(
                            Query.of(q1 -> q1.term(t -> t.field("entityId").value(entityId))),
                            Query.of(q2 -> q2.term(t -> t.field("operation").value(operation))),
                            Query.of(q3 -> q3.term(t -> t.field("operationSeq").value(operationSeq)))
                    ))
            ));
            requestBuilder.sort(s -> s.field(f -> f.field("syncAttempt").order(SortOrder.Desc)));
            requestBuilder.size(1);

            SearchResponse<EntityMetadata> response =
                    elasticsearchClient.search(requestBuilder.build(), EntityMetadata.class);

            return response.hits().hits().isEmpty() ? null : response.hits().hits().get(0).source();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
    public boolean existsByEntityIdAndOperationAndOperationSeqAndSyncAttempt(
            String entityId, String operation, long operationSeq, int syncAttempt) {
        try {
            SearchRequest request = new SearchRequest.Builder()
                    .index("entity_metadata")
                    .query(q -> q.bool(b -> b
                            .must(List.of(
                                    Query.of(q1 -> q1.term(t -> t.field("entityId.keyword").value(entityId))),
                                    Query.of(q2 -> q2.term(t -> t.field("operation.keyword").value(operation))),
                                    Query.of(q3 -> q3.term(t -> t.field("operationSeq").value(operationSeq))),
                                    Query.of(q4 -> q4.term(t -> t.field("syncAttempt").value(syncAttempt)))
                            ))
                    ))
                    .size(1)
                    .build();

            SearchResponse<EntityMetadata> response =
                    elasticsearchClient.search(request, EntityMetadata.class);

            return !response.hits().hits().isEmpty();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
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



