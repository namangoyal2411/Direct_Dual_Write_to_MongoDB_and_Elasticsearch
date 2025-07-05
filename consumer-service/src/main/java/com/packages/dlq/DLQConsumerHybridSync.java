package com.packages.dlq;

import com.packages.model.EntityEvent;
import com.packages.repository.EntityElasticRepository;
import com.packages.service.EntityMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumerHybridSync {
    private static final Logger log = LoggerFactory.getLogger(DLQConsumerHybridSync.class);
    private static final int MAX_RETRIES      = 5;
    private static final long MAX_BACKOFF_MS  = 150L;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
    private final EntityElasticRepository esRepo;
    private final EntityMetadataService   metadataService;
    @Autowired
    public DLQConsumerHybridSync(EntityElasticRepository esRepo,
                                 EntityMetadataService metadataService) {
        this.esRepo          = esRepo;
        this.metadataService = metadataService;
    }
    @KafkaListener(
            topics      = "dlq163",
            groupId     = "dlq-consumer-group",
            concurrency = "10"
    )
    public void consumeDLQ(EntityEvent event) {
        scheduler.submit(() -> retryInProcess(event, 0));
    }
    private void retryInProcess(EntityEvent event, int attempt) {
        log.info("DLQ retry attempt {} for entity {}", attempt, event.getEntity().getId());
        try {
            switch (event.getOperation()) {
                case "create" -> esRepo.createEntity("entity", event.getEntity());
                case "update", "delete" ->
                        esRepo.updateEntity("entity", event.getEntity().getId(), event.getEntity());
                default -> throw new IllegalArgumentException("Unknown op " + event.getOperation());
            }
            metadataService.updateEntityMetadata(
                    event.getMetadataId(),
                    "success",
                    System.currentTimeMillis(),
                    null
            );

        } catch (Exception ex) {
            if (attempt < MAX_RETRIES) {
                int    next           = attempt + 1;
                long   baseBackoff    = Math.min((1L << next) * 10, MAX_BACKOFF_MS);
                double jitterFactor   = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
                long   jitteredBackoff= Math.round(baseBackoff * jitterFactor);
                event.setRetryCount(next);
                log.warn("Scheduling in-process retry #{} in {}ms for {}", next, jitteredBackoff, event.getEntity().getId());
                scheduler.schedule(
                        () -> retryInProcess(event, next), //made a change here that rather than sending back to dlq topic doing in process retries
                        jitteredBackoff,
                        TimeUnit.MILLISECONDS
                );

            } else {
                metadataService.updateEntityMetadata(
                        event.getMetadataId(),
                        "failure",
                        null,
                        extractReason(ex)
                );
                log.error("Permanent DLQ failure for {} after {} attempts", event.getEntity().getId(), attempt, ex);
            }
        }
    }

    private String extractReason(Exception ex) {
        Throwable cause = ex;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String rootClass = cause.getClass().getSimpleName();
        String msg       = cause.getMessage() == null ? "" : cause.getMessage().toLowerCase();

        if ("ResponseException".equals(rootClass) || msg.contains("429")) {
            return "HTTP429";
        } else if ("ConnectionRequestTimeoutException".equals(rootClass) || msg.contains("connect timed out")) {
            return "ConnectTimeout";
        } else if ("SocketTimeoutException".equals(rootClass) || msg.contains("timeout")) {
            return "ReadTimeout";
        } else {
            return rootClass;
        }
    }
}
