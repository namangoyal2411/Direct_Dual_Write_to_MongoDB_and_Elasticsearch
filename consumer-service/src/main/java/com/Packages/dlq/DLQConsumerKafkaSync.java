package com.Packages.dlq;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
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
public class DLQConsumerKafkaSync {
    private static final int MAX_RETRIES = 5;
    private static final long MAX_BACKOFF_MS = 10L;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metaRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;

    public DLQConsumerKafkaSync(EntityElasticRepository esRepo,
                                EntityMetadataRepository metaRepo,
                                KafkaTemplate<String, EntityEvent> kafka) {
        this.esRepo = esRepo;
        this.metaRepo = metaRepo;
        this.kafka = kafka;
    }

    @KafkaListener(topics = "dlq113", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
        EntityMetadata meta = event.getEntityMetadata();
        int nextRetry = meta.getSyncAttempt() + 1;
        try {
            switch (event.getOperation()) {
                case "create" -> esRepo.createEntity(event.getIndex(), event.getEntity());
                case "update" -> esRepo.updateEntity(
                        event.getIndex(),
                        event.getId(),
                        event.getEntity(),
                        event.getEntity().getCreateTime()
                );
                case "delete" -> esRepo.deleteEntity(event.getIndex(), event.getId());
                default -> {
                }
            }
            meta.setSyncAttempt(nextRetry);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setDlqReason(null);
            metaRepo.update(meta.getMetaId(), meta);
        } catch (Exception ex) {
            handleRetryFailure(event, meta, nextRetry, ex);
        }
    }

    private void handleRetryFailure(EntityEvent event,
                                    EntityMetadata meta,
                                    int nextRetry,
                                    Exception ex) {
        if (meta.getFirstFailureTime() == null) {
            meta.setFirstFailureTime(System.currentTimeMillis());
        }
        String reason = (ex instanceof ElasticsearchException ee)
                ? ee.error().reason()
                : ex.getMessage();

        if (isClientError(ex)) {
            meta.setSyncAttempt(1);
            meta.setEsStatus("failure");
            meta.setEsSyncMillis(null);
            meta.setDlqReason(reason);
            metaRepo.update(meta.getMetaId(), meta);
            return;
        }

        meta.setSyncAttempt(nextRetry);
        meta.setEsStatus("failure");
        meta.setEsSyncMillis(null);
        meta.setDlqReason(reason);
        metaRepo.update(meta.getMetaId(), meta);

        if (nextRetry <= MAX_RETRIES) {
            long backoff = Math.min(1L << nextRetry, MAX_BACKOFF_MS);
            scheduler.schedule(
                    () -> kafka.send("dlq113", meta.getEntityId(), event),
                    backoff,
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private boolean isClientError(Exception ex) {
        return ex instanceof ElasticsearchException ee
                && ee.status() >= 400
                && ee.status() < 500;
    }
}


//package com.Packages.dlq;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityEvent;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
//@Service
//public class DLQConsumerKafkaSync {
//    public static final int maxRetries = 5;
//    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//    private final EntityElasticRepository elasticRepository;
//    private final EntityMetadataRepository metadataRepository;
//    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
//    public DLQConsumerKafkaSync(EntityElasticRepository elasticRepository,
//                                EntityMetadataRepository metadataRepository,
//                                KafkaTemplate<String, EntityEvent> kafkaTemplate) {
//        this.elasticRepository = elasticRepository;
//        this.metadataRepository = metadataRepository;
//        this.kafkaTemplate = kafkaTemplate;
//    }
//    @KafkaListener(topics = "dlq113", groupId = "dlq-consumer-group")
//    public void consumeDLQ(EntityEvent event) {
//        EntityMetadata metadata = event.getEntityMetadata();
//        int currentRetry = metadata.getSyncAttempt();
//        int nextRetry = currentRetry + 1;
//        try {
//            Entity entity = event.getEntity();
//            String op = event.getOperation();
//            String idx = event.getIndex();
//            String id = event.getId();
//            switch (op) {
//                case "create" -> elasticRepository.createEntity(idx, entity);
//                case "update" -> elasticRepository.updateEntity(idx, id, entity, entity.getCreateTime());
//                case "delete" -> elasticRepository.deleteEntity(idx, id);
//                default -> {
//                    return;
//                }
//            }
//            metadata.setSyncAttempt(nextRetry);
//            metadata.setEsStatus("success");
//            metadata.setEsSyncMillis(System.currentTimeMillis());
//            metadata.setDlqReason(null);
//            metadataRepository.update(metadata.getMetaId(), metadata);
//        } catch (Exception ex) {
//            handleRetryFailure(event, metadata, nextRetry, ex);
//        }
//    }
//    private void handleRetryFailure(EntityEvent event,
//                                    EntityMetadata metadata,
//                                    int nextRetry,
//                                    Exception ex) {
//        String reason;
//        if (ex instanceof ElasticsearchException ee) {
//            reason = ee.error().reason();
//        } else {
//            reason = ex.getMessage();
//        }
//        if (ex instanceof ElasticsearchException ee
//                && ee.status() >= 400 && ee.status() < 500) {
//            metadata.setEsStatus("failure");
//            metadata.setDlqReason(reason);
//            metadata.setSyncAttempt(1);
//            metadata.setEsSyncMillis(null);
//            metadataRepository.update(metadata.getMetaId(), metadata);
//            return;
//        }
//        if (nextRetry > maxRetries) {
//            metadata.setSyncAttempt(nextRetry);
//            metadata.setEsStatus("failure");
//            metadata.setEsSyncMillis(null);
//            metadata.setDlqReason(reason);
//            metadataRepository.update(metadata.getMetaId(), metadata);
//            return;
//        }
//        metadata.setSyncAttempt(nextRetry);
//        metadata.setEsStatus("failure");
//        metadata.setEsSyncMillis(null);
//        metadata.setDlqReason(reason);
//        metadataRepository.update(metadata.getMetaId(), metadata);
//        long backoffMillis = Math.min((long) Math.pow(2, nextRetry), 10L);
//        scheduler.schedule(() -> {
//            kafkaTemplate.send("dlq113", metadata.getEntityId(), event);
//        }, backoffMillis, TimeUnit.MILLISECONDS);
//    }
//}