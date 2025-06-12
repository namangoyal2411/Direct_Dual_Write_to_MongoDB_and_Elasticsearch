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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumer {

    private static final int MAX_RETRIES = 5;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
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

    @KafkaListener(topics = "dlq-entity101", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
        String entityMetadataId = event.getMetadataId();
        EntityMetadata metadata = metadataRepository.getById(entityMetadataId);

        int currentRetry = metadata.getSyncAttempt();
        int nextRetry = currentRetry + 1;
        try {
            Entity entity = event.getEntity();
            String op = event.getOperation();
            String idx = event.getIndex();
            String id = event.getId();
            switch (op) {
                case "create" -> elasticRepository.createEntity(idx, entity);
                case "update" -> elasticRepository.updateEntity(idx, id, entity, entity.getCreateTime());
                case "delete" -> elasticRepository.deleteEntity(idx, id);
                default -> {
                    System.err.println("Unsupported operation: " + op);
                    return;
                }
            }
            metadata.setSyncAttempt(nextRetry);
            metadata.setEsStatus("SUCCESS");
            metadata.setEsSyncMillis(System.currentTimeMillis());
            metadata.setDlqReason(null);
            metadataRepository.update(metadata.getMetaId(),metadata);

        } catch (Exception ex) {
            handleRetryFailure(event, metadata, nextRetry, ex);
        }
    }
    private void handleRetryFailure(EntityEvent event,
                                    EntityMetadata metadata,
                                    int nextRetry,
                                    Exception ex) {
        if (nextRetry > MAX_RETRIES) {
            System.err.println("Giving up retries for " + metadata.getEntityId());
            return;
        }

        String reason;
            reason = "Retry failed: " + ex.getMessage();


        metadata.setSyncAttempt(nextRetry);
        metadata.setEsStatus("FAILURE");
        metadata.setEsSyncMillis(null);
        metadata.setDlqReason(reason);
        metadataRepository.update(metadata.getMetaId(), metadata);
        long backoffMillis = Math.min((long) Math.pow(2, nextRetry) * 1000L, 10000L); // Max 10s
        scheduler.schedule(() -> {
            kafkaTemplate.send("dlq-entity101", metadata.getEntityId(), event);
            System.out.println("Requeued DLQ for retry " + nextRetry);
        }, backoffMillis, TimeUnit.MILLISECONDS);
    }
    }

