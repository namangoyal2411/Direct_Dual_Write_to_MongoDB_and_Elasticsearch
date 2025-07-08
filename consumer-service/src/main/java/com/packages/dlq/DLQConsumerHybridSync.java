package com.packages.dlq;

import com.packages.model.EntityEvent;
import com.packages.repository.EntityElasticRepository;
import com.packages.service.EntityMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@Service
public class DLQConsumerHybridSync {

    private static final Logger log = LoggerFactory.getLogger(DLQConsumerHybridSync.class);

    private final EntityElasticRepository esRepo;
    private final EntityMetadataService   metadataService;

    public DLQConsumerHybridSync(EntityElasticRepository esRepo,
                                 EntityMetadataService metadataService) {
        this.esRepo = esRepo;
        this.metadataService = metadataService;
    }

    @RetryableTopic(
            attempts               = "5",
            backoff                = @Backoff(delay = 1_000, multiplier = 2.0, maxDelay = 30_000),
            autoCreateTopics       = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            numPartitions          = "10"
    )
    @KafkaListener(
            topics     = "dlq178",
            groupId    = "dlq-consumer-group",
            concurrency = "10"
    )
    public void consumeDLQ(EntityEvent event) {

        log.info("Processing DLQ event for entity {} op={}",
                event.getEntity().getId(), event.getOperation());

        switch (event.getOperation()) {
            case "create" ->
                    esRepo.createEntity("entity", event.getEntity());

            case "update", "delete" ->
                    esRepo.updateEntity("entity",
                            event.getEntity().getId(),
                            event.getEntity());

            default ->
                    throw new IllegalArgumentException(
                            "Unknown op " + event.getOperation());
        }

        metadataService.updateEntityMetadata(
                event.getMetadataId(),
                "success",
                System.currentTimeMillis(),
                null
        );
    }

    @DltHandler
    public void processFailure(
            EntityEvent event,
            @Header(KafkaHeaders.DLT_ORIGINAL_TOPIC) String originalTopic,
            @Header("kafka_dlt-exception-fqcn") String exceptionClassName,
            @Header("kafka_dlt-exception-message") String exceptionMessage
    ) {
        String reason = classify(exceptionClassName, exceptionMessage);

        log.error("DLQ exhausted for entity {} on {} → classified reason={}",
                event.getEntity().getId(), originalTopic, reason);

        metadataService.updateEntityMetadata(
                event.getMetadataId(),
                "failure",
                null,
                reason
        );
    }

    private String classify(String rootFqcn, String message) {
        String root = rootFqcn == null
                ? "UnknownException"
                : rootFqcn.substring(rootFqcn.lastIndexOf('.') + 1);

        String msg = message == null ? "" : message.toLowerCase();

        if ("ResponseException".equals(root) || msg.contains("429") || msg.contains("too many requests")) {
            return "HTTP429";
        }
        if ("ConnectionRequestTimeoutException".equals(root) || msg.contains("connect timed out")) {
            return "ConnectTimeout";
        }
        if ("SocketTimeoutException".equals(root)
                || msg.contains("timeout on connection")
                || msg.contains("read timeout")) {
            return "ReadTimeout";
        }
        return root;
    }
}
