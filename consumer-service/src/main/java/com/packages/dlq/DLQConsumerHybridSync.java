package com.packages.dlq;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.model.EntityEvent;
import com.packages.model.EntityMetadata;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMetadataMongoRepository;
import com.packages.repository.EntityMetadataRepository;
import com.packages.repository.EntityMongoRepository;
import com.packages.service.EntityMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumerHybridSync {
    private static final int MAX_RETRIES = 5;
    private static final long MAX_BACKOFF_MS = 10L;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final EntityElasticRepository esRepo;
    private final EntityMetadataMongoRepository metadataMongoRepository;
    private final KafkaTemplate<String, EntityEvent> kafka;
    private final EntityMetadataService metadataService;
    private static final Logger log =
            LoggerFactory.getLogger(DLQConsumerHybridSync.class);
    @Autowired
    public DLQConsumerHybridSync(EntityElasticRepository esRepo,
                                 KafkaTemplate<String, EntityEvent> kafka,EntityMetadataMongoRepository metadataMongoRepository,EntityMetadataService metadataService) {
        this.esRepo = esRepo;
        this.kafka = kafka;
        this.metadataMongoRepository = metadataMongoRepository;
        this.metadataService = metadataService;
    }
    @KafkaListener(topics = "dlq130", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEvent event) {
//        EntityMetadata meta = metadataMongoRepository.getEntityMetadata(event.getMetadataId())
//                .orElseThrow(() -> new EntityNotFoundException(event.getMetadataId()));
        int retryCount = event.getRetryCount();
        log.info("retry count = {}", retryCount);
        try {
            switch (event.getOperation()) {
                case "create" -> esRepo.createEntity("entity", event.getEntity());
                case "update" -> esRepo.updateEntity("entity",
                        event.getEntity().getId(),
                        event.getEntity());
                case "delete" -> esRepo.deleteEntity("entity",
                        event.getEntity().getId());
                default -> throw new IllegalArgumentException("Unknown op " + event.getOperation());
            }
            metadataService.updateEntityMetadata(
                    event.getMetadataId(), "success", System.currentTimeMillis(), null
            );

        } catch (Exception ex) {
            if (retryCount < MAX_RETRIES) {
                int next = retryCount + 1;
                long backoff = Math.min(1L << next, MAX_BACKOFF_MS);
                event.setRetryCount(next);
                scheduler.schedule(
                        () -> kafka.send("dlq130", event),
                        backoff,
                        TimeUnit.MILLISECONDS
                );
            } else {
                metadataService.updateEntityMetadata(
                        event.getMetadataId(), "failure", null, extractReason(ex)
                );
            }
        }
    }



    private String extractReason(Exception ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String rootClass = cause.getClass().getSimpleName();
        String msg       = cause.getMessage() == null
                ? ""
                : cause.getMessage().toLowerCase();
        String reason;
        if ("ResponseException".equals(rootClass)
                || msg.contains("429")
                || msg.contains("too many requests")) {
            reason = "HTTP429";
        } else if ("ConnectionRequestTimeoutException".equals(rootClass)
                || msg.contains("connect timed out")) {
            reason = "ConnectTimeout";
        } else if ("SocketTimeoutException".equals(rootClass)
                || msg.contains("timeout on connection")
                || msg.contains("read timeout")) {
            reason = "ReadTimeout";
        } else {
            // any other root cause (e.g. some other IO error)
            reason = rootClass;
        }
        return reason;
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
//public class DLQConsumerHybridSync {
//    public static final int maxRetries = 5;
//    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//    private final EntityElasticRepository elasticRepository;
//    private final EntityMetadataRepository metadataRepository;
//    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
//    public DLQConsumerHybridSync(EntityElasticRepository elasticRepository,
//                                EntityMetadataRepository metadataRepository,
//                                KafkaTemplate<String, EntityEvent> kafkaTemplate) {
//        this.elasticRepository = elasticRepository;
//        this.metadataRepository = metadataRepository;
//        this.kafkaTemplate = kafkaTemplate;
//    }
//    @KafkaListener(topics = "dlq114", groupId = "dlq-consumer-group")
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
//            kafkaTemplate.send("dlq114", metadata.getEntityId(), event);
//        }, backoffMillis, TimeUnit.MILLISECONDS);
//    }
//}