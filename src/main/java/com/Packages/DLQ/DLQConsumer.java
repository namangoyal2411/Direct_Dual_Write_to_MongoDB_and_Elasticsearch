package com.Packages.DLQ;

import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Model.EntityMetadata;
import com.Packages.Repository.EntityElasticRepository;
import com.Packages.Repository.EntityMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class DLQConsumer {

    private static final int MAX_RETRIES = 5;

    private final EntityElasticRepository elasticRepository;
    private final EntityMetadataRepository metadataRepository;
    private final DLQElasticRepository dlqElasticRepository;
    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;

    @Autowired
    public DLQConsumer(EntityElasticRepository elasticRepository,
                       EntityMetadataRepository metadataRepository,
                       DLQElasticRepository dlqElasticRepository,
                       KafkaTemplate<String, EntityEvent> kafkaTemplate) {
        this.elasticRepository = elasticRepository;
        this.metadataRepository = metadataRepository;
        this.dlqElasticRepository = dlqElasticRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "dlq-entity14", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
        EntityMetadata originalMeta = event.getMetadata();
        if (originalMeta == null) return;
        EntityMetadata latestMeta = metadataRepository.getEntityMetaData(originalMeta.getEntityId(), originalMeta.getOperation(), originalMeta.getOperationSeq());
        int currentRetry = latestMeta != null ? latestMeta.getSyncAttempt() : 0;
        try {
            Thread.sleep((long) Math.pow(2, currentRetry) * 1000);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
        }

        try {
            EntityDTO dto = event.getEntityDTO();
            String op = event.getOperation();
            String idx = event.getIndex();
            String id = event.getId();

            switch (op) {
                case "create" ->
                        elasticRepository.createEntity(idx, Entity.fromDTO(dto));
                case "update" ->
                        elasticRepository.updateEntity(idx, id, Entity.fromDTO(dto), dto.getCreateTime());
                case "delete" ->
                        elasticRepository.deleteEntity(idx, id);
                default -> {
                    System.err.println("Unsupported operation: " + op);
                    return;
                }
            }
            EntityMetadata successMeta = EntityMetadata.builder()
                    .metaId(UUID.randomUUID().toString())
                    .entityId(originalMeta.getEntityId())
                    .operation(op)
                    .operationSeq(originalMeta.getOperationSeq())
                    .mongoWriteMillis(originalMeta.getMongoWriteMillis())
                    .esSyncMillis(System.currentTimeMillis())
                    .syncAttempt(currentRetry + 1)
                    .mongoStatus(originalMeta.getMongoStatus())
                    .esStatus("SUCCESS")
                    .dlqReason(null)
                    .build();
            metadataRepository.save(successMeta);
            dlqElasticRepository.saveDLQEvent("entity_dlq", event);

        } catch (Exception ex) {
            handleRetryFailure(event, originalMeta, currentRetry, ex);
        }
    }

    private void handleRetryFailure(EntityEvent event,
                                    EntityMetadata originalMeta,
                                    int currentRetry,
                                    Exception ex) {
        String reason;
        if (isInvalidDataError(ex)) {
            reason = "Invalid data: " + ex.getMessage();
        } else if (currentRetry >= MAX_RETRIES) {
            reason = "Max retries reached: " + ex.getMessage();
        } else {
            reason = "Failure due to: " + ex.getMessage();
        }
        EntityMetadata retryMeta = EntityMetadata.builder()
                .metaId(UUID.randomUUID().toString())
                .entityId(originalMeta.getEntityId())
                .operation(originalMeta.getOperation())
                .operationSeq(originalMeta.getOperationSeq())
                .mongoWriteMillis(originalMeta.getMongoWriteMillis())
                .esSyncMillis(null)
                .syncAttempt(currentRetry + 1)
                .mongoStatus(originalMeta.getMongoStatus())
                .esStatus("FAILURE")
                .dlqReason(reason)
                .build();
        metadataRepository.save(retryMeta);
        event.setMetadata(retryMeta);
        if (currentRetry + 1 < MAX_RETRIES) {
            kafkaTemplate.send("dlq-entity14", retryMeta.getEntityId(), event);
        } else {
            System.err.println("Giving up retries for " + retryMeta.getEntityId());
        }
    }

    private boolean isInvalidDataError(Exception e) {
        String msg = e.getMessage();
        if (msg == null) return false;
        msg = msg.toLowerCase();
        return msg.contains("mapper_parsing_exception") ||
                msg.contains("validation") ||
                msg.contains("illegal_argument");
    }
}
