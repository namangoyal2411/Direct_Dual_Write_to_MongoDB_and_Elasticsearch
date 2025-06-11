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

import java.util.UUID;

@Service
public class DLQConsumer {

    private final EntityElasticRepository entityElasticRepository;
    private final EntityMetadataRepository entityMetadataRepository;
    private final DLQElasticRepository dlqElasticRepository;
    @Autowired
    public DLQConsumer(EntityElasticRepository entityElasticRepository,
                       EntityMetadataRepository entityMetadataRepository,
                       DLQElasticRepository dlqElasticRepository) {
        this.entityElasticRepository = entityElasticRepository;
        this.entityMetadataRepository = entityMetadataRepository;
        this.dlqElasticRepository= dlqElasticRepository;
    }

    @KafkaListener(topics = "dlq-entity10", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent failedEvent) {
        int maxRetries = 5;
        int currentRetryCount = 0;
        EntityMetadata metadata = failedEvent.getMetadata();
        if (metadata != null) {
            currentRetryCount = metadata.getSyncAttempt();
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
                default:
                    System.err.println("Unsupported operation: " + operation);
                    return;
            }
            if (metadata != null) {
                EntityMetadata successMeta = EntityMetadata.builder()
                        .metaId(UUID.randomUUID().toString())
                        .entityId(metadata.getEntityId())
                        .operation(metadata.getOperation())
                        .operationSeq(metadata.getOperationSeq())
                        .mongoWriteMillis(metadata.getMongoWriteMillis())
                        .esSyncMillis(System.currentTimeMillis())
                        .syncAttempt(currentRetryCount + 1)
                        .mongoStatus(metadata.getMongoStatus())
                        .esStatus("Success")
                        .dlqReason(null)
                        .build();
                entityMetadataRepository.save(successMeta);
            }
            dlqElasticRepository.saveDLQEvent("entity_dlq", failedEvent);
        } catch (Exception e) {
            handleProcessingFailure(failedEvent, currentRetryCount, maxRetries, e);
        }
    }

    private void handleProcessingFailure(EntityEvent failedEvent, int currentRetryCount, int maxRetries, Exception e) {
        EntityMetadata metadata = failedEvent.getMetadata();

        if (metadata != null) {String dlqReason;
            if (isInvalidDataError(e)) {
                dlqReason = "Invalid data: " + e.getMessage();
            } else if (currentRetryCount >= maxRetries) {
                dlqReason = "Max retries reached: " + e.getMessage();
            } else {
                dlqReason = "Failure due to: " + e.getMessage();
            }

            EntityMetadata retryMetadata = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(metadata.getEntityId())
                    .operation(metadata.getOperation())
                    .operationSeq(metadata.getOperationSeq())
                    .mongoWriteMillis(metadata.getMongoWriteMillis())
                    .esSyncMillis(System.currentTimeMillis())
                    .syncAttempt(currentRetryCount + 1)
                    .mongoStatus(metadata.getMongoStatus())
                    .esStatus("failure")
                    .dlqReason(dlqReason)
                    .build();
            entityMetadataRepository.save(retryMetadata);
        }
    }

    public boolean isInvalidDataError(Exception e) {
        if (e.getMessage() == null) return false;
        String message = e.getMessage().toLowerCase();
        return message.contains("mapper_parsing_exception")
                || message.contains("validation")
                || message.contains("illegal_argument");
    }
}
