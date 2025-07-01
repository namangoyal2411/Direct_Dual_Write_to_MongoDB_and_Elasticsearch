package com.packages.dlq;
import com.packages.model.EntityEvent;
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
        int retryCount = event.getRetryCount();
        log.info("retry count = {}", retryCount);
        try {
            switch (event.getOperation()) {
                case "create" -> esRepo.createEntity("entity", event.getEntity());
                case "update","delete" -> esRepo.updateEntity("entity",
                        event.getEntity().getId(),
                        event.getEntity());
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
            reason = rootClass;
        }
        return reason;
    }
}
