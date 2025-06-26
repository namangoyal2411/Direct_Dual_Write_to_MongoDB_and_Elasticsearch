package com.Packages.kafka;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EntityConsumer {

    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metaRepo;
    private final KafkaTemplate<String, EntityEvent> kafka;

    public EntityConsumer(EntityElasticRepository esRepo,
                          EntityMetadataRepository metaRepo,
                          KafkaTemplate<String, EntityEvent> kafka) {
        this.esRepo = esRepo;
        this.metaRepo = metaRepo;
        this.kafka = kafka;
    }

    @KafkaListener(topics = "entity113", groupId = "es-consumer-group")
    public void consume(EntityEvent event) {
        EntityMetadata meta = event.getEntityMetadata();
        try {
            applyOperation(event);
            markSuccess(meta);
            metaRepo.update(meta.getMetaId(), meta);
        } catch (Exception ex) {
            handleFailure(event, meta, ex);
        }
    }

    private void applyOperation(EntityEvent event) {
        String op = event.getOperation();
        String idx = event.getIndex();
        String id = event.getId();
        Entity ent = event.getEntity();
        switch (op) {
            case "create" -> esRepo.createEntity(idx, ent);
            case "update" -> esRepo.updateEntity(idx, id, ent, ent.getCreateTime());
            case "delete" -> esRepo.deleteEntity(idx, id);
            default -> throw new IllegalArgumentException("Unknown operation: " + op);
        }
    }

    private void markSuccess(EntityMetadata meta) {
        meta.setEsSyncMillis(System.currentTimeMillis());
        meta.setEsStatus("success");
        meta.setSyncAttempt(1);
    }

    private void handleFailure(EntityEvent event, EntityMetadata meta, Exception ex) {
        if (meta.getFirstFailureTime() == null) {
            meta.setFirstFailureTime(System.currentTimeMillis());
        }
        String reason = (ex instanceof ElasticsearchException ee)
                ? ee.error().reason()
                : ex.getMessage();
        meta.setDlqReason(reason);

        if (ex instanceof ElasticsearchException ee
                && ee.status() >= 400 && ee.status() < 500) {
            markFailure(meta);
            metaRepo.update(meta.getMetaId(), meta);
            return;
        }

        markFailure(meta);
        metaRepo.update(meta.getMetaId(), meta);
        sendToDLQ(event);
    }

    private void markFailure(EntityMetadata meta) {
        meta.setEsStatus("failure");
        meta.setEsSyncMillis(null);
        meta.setSyncAttempt(1);
    }

    private void sendToDLQ(EntityEvent event) {
        try {
            kafka.send("dlq113", event.getEntity().getId(), event);
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
        }
    }
}

//
//package com.Packages.kafka;
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityEvent;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//@Service
//public class EntityConsumer {
//    private final EntityElasticRepository entityElasticRepository;
//    private final EntityMetadataRepository entityMetadataRepository;
//    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
//    @Autowired
//    public EntityConsumer(EntityElasticRepository entityElasticRepository,
//                          EntityMetadataRepository entityMetadataRepository,
//                          KafkaTemplate<String, EntityEvent> kafkaTemplate) {
//        this.entityElasticRepository = entityElasticRepository;
//        this.entityMetadataRepository = entityMetadataRepository;
//        this.kafkaTemplate = kafkaTemplate;
//    }
//    @KafkaListener(topics = "entity113", groupId = "es-consumer-group")
//    public void Consume(EntityEvent entityEvent) {
//        EntityMetadata metadata = entityEvent.getEntityMetadata();
//        try {
//            Entity entity = entityEvent.getEntity();
//            String operation = entityEvent.getOperation();
//            String indexName = entityEvent.getIndex();
//            String documentId = entityEvent.getId();
//            switch (operation) {
//                case "create":
//                    entityElasticRepository.createEntity(
//                            indexName,
//                            entity
//                    );
//                    break;
//                case "update":
//                    entityElasticRepository.updateEntity(
//                            indexName,
//                            documentId,
//                            entity,
//                            entity.getCreateTime()
//                    );
//                    break;
//                case "delete":
//                    entityElasticRepository.deleteEntity(indexName, documentId);
//                    break;
//            }
//            metadata.setEsSyncMillis(System.currentTimeMillis());
//            metadata.setEsStatus("success");
//            metadata.setSyncAttempt(1);
//            entityMetadataRepository.update(metadata.getMetaId(), metadata);
//        } catch (Exception e) {
//            if (metadata.getFirstFailureTime() == null) {
//                metadata.setFirstFailureTime(System.currentTimeMillis());
//            }
//            String reason;
//            if (e instanceof ElasticsearchException ee) {
//                reason = ee.error().reason();
//            } else {
//                reason = e.getMessage();
//            }
//            if (e instanceof ElasticsearchException ee
//                    && ee.status() >= 400 && ee.status() < 500) {
//                metadata.setEsStatus("failure");
//                metadata.setDlqReason(reason);
//                metadata.setSyncAttempt(1);
//                metadata.setEsSyncMillis(null);
//                entityMetadataRepository.update(metadata.getMetaId(), metadata);
//                return;
//            }
//            metadata.setEsStatus("failure");
//            metadata.setDlqReason(reason);
//            metadata.setSyncAttempt(1);
//            metadata.setEsSyncMillis(null);
//            entityMetadataRepository.update(metadata.getMetaId(), metadata);
//            sendToDLQ(entityEvent, e);
//        }
//    }
/// / take topic different for different approach like kafka or hybrid
//    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
//        try {
//            kafkaTemplate.send("dlq113", failedEvent.getEntity().getId(), failedEvent);
//            System.out.println("Message sent to DLQ: " + failedEvent);
//        } catch (Exception ex) {
//            System.err.println("Failed to send to DLQ: " + ex.getMessage());
//        }
//    }
//
//}