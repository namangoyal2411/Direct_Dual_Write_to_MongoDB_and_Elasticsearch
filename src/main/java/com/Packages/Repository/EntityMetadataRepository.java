package com.Packages.Repository;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
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
                    .document(entityMetadata)
            );
        }
            catch (Exception e){
                e.printStackTrace();
            }

        }
    }

