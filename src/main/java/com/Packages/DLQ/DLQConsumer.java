package com.Packages.DLQ;

import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Model.EntityMetadata;
import com.Packages.Repository.EntityElasticRepository;
import com.Packages.Repository.EntityMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DLQConsumer {

    private final EntityElasticRepository entityElasticRepository;
    private final EntityMetadataRepository entityMetadataRepository;

    @Autowired
    public DLQConsumer(EntityElasticRepository entityElasticRepository,
                       EntityMetadataRepository entityMetadataRepository) {
        this.entityElasticRepository = entityElasticRepository;
        this.entityMetadataRepository = entityMetadataRepository;
    }

    @KafkaListener(topics = "dlq-entity", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent failedEvent) {
        try {
            EntityDTO entityDTO = failedEvent.getEntityDTO();
            String operation = failedEvent.getOperation();
            String indexName = failedEvent.getIndex();
            String documentId = failedEvent.getId();
            switch (operation) {
                case "create":
                    entityElasticRepository.createEntity(indexName, Entity.fromDTO(entityDTO));
                    break;
                case "update":
                    entityElasticRepository.updateEntity(indexName, documentId, Entity.fromDTO(entityDTO), entityDTO.getCreateTime());
                    break;
                case "delete":
                    entityElasticRepository.deleteEntity(indexName, documentId);
                    break;
            }
            EntityMetadata entityMetadata = failedEvent.getMetadata();
            entityMetadata.setEsSyncMillis(System.currentTimeMillis());
            entityMetadata.setSyncAttempt(entityMetadata.getSyncAttempt() + 1);
            entityMetadata.setEsStatus("SUCCESS");
            entityMetadataRepository.save(entityMetadata);
            System.out.println("Successfully retried DLQ message for " + failedEvent.getId());
        }
         catch (Exception e) {
            System.err.println("Failed to retry DLQ message: " + failedEvent.getId() + " due to: " + e.getMessage());
        }
    }
}
