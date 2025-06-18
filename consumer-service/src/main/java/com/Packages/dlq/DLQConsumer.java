package com.Packages.dlq;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumer {
    public static final int MAX_RETRIES = 5;
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
    @KafkaListener(topics = "dlq81", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
//        log.info("[DLQConsumer] got event id={} attempt={}",
//                event.getId(),
//                event.getEntityMetadata().getSyncAttempt());
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
        String reason;
        if (ex instanceof ElasticsearchException ee) {
            reason = ee.error().reason();
        } else {
            reason = ex.getMessage();
        }
        if (ex instanceof ElasticsearchException ee
                && ee.status() >= 400 && ee.status() < 500) {
            metadata.setEsStatus("failure");
            metadata.setDlqReason(reason);
            metadata.setSyncAttempt(1);
            metadata.setEsSyncMillis(null);
            metadataRepository.update(metadata.getMetaId(), metadata);
            return;
        }
        if (nextRetry > MAX_RETRIES) {
            metadata.setSyncAttempt(nextRetry);
            metadata.setEsStatus("failure");
            metadata.setEsSyncMillis(null);
            metadata.setDlqReason(reason);
            metadataRepository.update(metadata.getMetaId(), metadata);
            return;
        }
        metadata.setSyncAttempt(nextRetry);
        metadata.setEsStatus("failure");
        metadata.setEsSyncMillis(null);
        metadata.setDlqReason(reason);
        metadataRepository.update(metadata.getMetaId(), metadata);
        long backoffMillis = Math.min((long) Math.pow(2, nextRetry), 10L);
        scheduler.schedule(() -> {
            kafkaTemplate.send("dlq81", metadata.getEntityId(), event);
        }, backoffMillis, TimeUnit.MILLISECONDS);
    }
}