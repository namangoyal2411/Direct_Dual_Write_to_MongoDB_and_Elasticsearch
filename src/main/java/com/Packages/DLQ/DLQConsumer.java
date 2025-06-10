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
        int maxRetries = 5;
        int currentRetryCount = failedEvent.getMetadata().getSyncAttempt();

        if (currentRetryCount > maxRetries) {
            return;
        }
        try {
            long backoffMillis = (long) Math.pow(2, currentRetryCount) * 1000;
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
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
            EntityMetadata metadata = failedEvent.getMetadata();
            metadata.setEsSyncMillis(System.currentTimeMillis());
            metadata.setSyncAttempt(metadata.getSyncAttempt() + 1);
            metadata.setEsStatus("Success");
            metadata.setDlqReason(null);
            entityMetadataRepository.save(metadata);
        }
        catch (Exception e) {
            EntityMetadata metadata = failedEvent.getMetadata();
            metadata.setSyncAttempt(metadata.getSyncAttempt() + 1);
            metadata.setEsSyncMillis(System.currentTimeMillis());
            if (isInvalidDataError(e)) {
                metadata.setEsStatus("failure");
                metadata.setDlqReason("Invalid data: " + e.getMessage());
                return ;
            }   else if (currentRetryCount >= maxRetries) {
                metadata.setEsStatus("failure after max retries");
                metadata.setDlqReason("Max retries reached " + e.getMessage());
                entityMetadataRepository.save(metadata);
                return;
            }
            else {
                    metadata.setEsStatus("failure");
                    metadata.setDlqReason("failure due to" + e.getMessage());
                    entityMetadataRepository.save(metadata);
                }
            }


        }

public boolean isInvalidDataError(Exception e){
    String message = e.getMessage().toLowerCase();
    return message.contains("mapper_parsing_exception") || message.contains("validation") || message.contains("illegal_argument");
}
}

