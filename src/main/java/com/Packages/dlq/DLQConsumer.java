package com.Packages.dlq;

import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumer {
    private static final int MAX_RETRIES = 5;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final EntityElasticRepository elasticRepository;
    private final EntityMetadataRepository metadataRepository;
    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
    public DLQConsumer(EntityElasticRepository elasticRepository,
                       EntityMetadataRepository metadataRepository,
                       KafkaTemplate<String, EntityEvent> kafkaTemplate) {
        this.elasticRepository = elasticRepository;
        this.metadataRepository = metadataRepository;
        this.kafkaTemplate = kafkaTemplate;
    }
    @KafkaListener(topics = "dlq35", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
        //String entityMetadataId = event.getMetadataId();
        EntityMetadata metadata = event.getEntityMetadata();
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
                    return;
                }
            }
            metadata.setSyncAttempt(nextRetry);
            metadata.setEsStatus("success");
            metadata.setEsSyncMillis(System.currentTimeMillis());
            metadata.setDlqReason(null);
           // metadataRepository.save(metadata);
            metadataRepository.update(metadata.getMetaId(), metadata);
        } catch (Exception ex) {
            handleRetryFailure(event, metadata, nextRetry, ex);
        }
    }
    private void handleRetryFailure(EntityEvent event,
                                    EntityMetadata metadata,
                                    int nextRetry,
                                    Exception ex) {
        if (nextRetry > MAX_RETRIES) {
            return;
        }
        String reason;
        reason = ex.getMessage();
        metadata.setSyncAttempt(nextRetry);
        metadata.setEsStatus("failure");
        metadata.setEsSyncMillis(null);
        metadata.setDlqReason(reason);
        //metadataRepository.save(metadata);
        metadataRepository.update(metadata.getMetaId(), metadata);
        long backoffMillis = Math.min((long) Math.pow(2, nextRetry), 10L);
        scheduler.schedule(() -> {
            kafkaTemplate.send("dlq35", metadata.getEntityId(), event);
        }, backoffMillis, TimeUnit.MILLISECONDS);
    }
}