
package com.Packages.kafka;

import com.Packages.model.Entity;
import com.Packages.model.EntityEvent;
import com.Packages.model.EntityMetadata;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EntityConsumer {
    private final EntityElasticRepository entityElasticRepository;
    private final EntityMetadataRepository entityMetadataRepository;
    private final KafkaTemplate<String, EntityEvent> kafkaTemplate;
    @Autowired
    public EntityConsumer(EntityElasticRepository entityElasticRepository,
                          EntityMetadataRepository entityMetadataRepository,
                          KafkaTemplate<String, EntityEvent> kafkaTemplate) {
        this.entityElasticRepository = entityElasticRepository;
        this.entityMetadataRepository = entityMetadataRepository;
        this.kafkaTemplate = kafkaTemplate;
    }
    @KafkaListener(topics = "Entity2700", groupId = "es-consumer-group")
    public void Consume(EntityEvent entityEvent){
        String metaId = entityEvent.getMetadataId();
        EntityMetadata metadata = entityMetadataRepository.getById(metaId);
        if (metadata == null) {
            return;
        }
        try {
            Entity entity = entityEvent.getEntity();
            String operation = entityEvent.getOperation();
            String indexName = entityEvent.getIndex();
            String documentId = entityEvent.getId();
            switch (operation) {
                case "create":
                    entityElasticRepository.createEntity(
                            indexName,
                            entity
                    );
                    break;
                case "update":
                    entityElasticRepository.updateEntity(
                            indexName,
                            documentId,
                            entity,
                            entity.getCreateTime()
                    );
                    break;
                case "delete":
                    entityElasticRepository.deleteEntity(indexName, documentId);
                    break;
            }
            metadata.setEsSyncMillis(System.currentTimeMillis());
            metadata.setEsStatus("success");
            metadata.setSyncAttempt(1);
            entityMetadataRepository.update(metaId, metadata);
        }
        catch (Exception e) {
            System.err.println("Failed to save data in Elasticsearch"+e.getMessage());
            if (metadata.getFirstFailureTime() == null) {
                metadata.setFirstFailureTime(System.currentTimeMillis());
            }
            if (isInvalidDataError(e)) {
                metadata.setEsStatus("failure");
                metadata.setDlqReason("Invalid data: " + e.getMessage());
                metadata.setSyncAttempt(1);
                metadata.setEsSyncMillis(null);
                entityMetadataRepository.update(metadata.getMetaId(), metadata);
                System.err.println("Not sending to DLQ due to bad data: " + e.getMessage());
                return;
            }
            metadata.setEsStatus("failure");
            metadata.setDlqReason("Initial sync failed: " + e.getMessage());
            metadata.setSyncAttempt(1);
            metadata.setEsSyncMillis(null);
            entityMetadataRepository.update(metadata.getMetaId(), metadata);
            sendToDLQ(entityEvent, e);
        }
    }
    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
        try {
            kafkaTemplate.send("dlq-entity2700",failedEvent.getEntity().getId(), failedEvent);
            System.out.println("Message sent to DLQ: " + failedEvent);
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
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
