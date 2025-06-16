
package com.Packages.kafka;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
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

    @KafkaListener(topics = "entity35", groupId = "es-consumer-group")
    public void Consume(EntityEvent entityEvent) {
      //  String metaId = entityEvent.getMetadataId();
        EntityMetadata metadata = entityEvent.getEntityMetadata();
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
           // entityMetadataRepository.save(metadata);
            entityMetadataRepository.update(metadata.getMetaId(), metadata);
        } catch (Exception e) {
            if (metadata.getFirstFailureTime() == null) {
                metadata.setFirstFailureTime(System.currentTimeMillis());
            }
            String reason;
            if (e instanceof ElasticsearchException ee) {
                reason = ee.error().reason();
            } else {
                reason = e.getMessage();
            }
            if (e instanceof ElasticsearchException ee
                    && ee.status() >= 400 && ee.status() < 500) {
                metadata.setEsStatus("failure");
                metadata.setDlqReason(reason);
                metadata.setSyncAttempt(1);
                metadata.setEsSyncMillis(null);
               // entityMetadataRepository.save(metadata);
                entityMetadataRepository.update(metadata.getMetaId(), metadata);
                return;
            }
            metadata.setEsStatus("failure");
            metadata.setDlqReason(e.getMessage());
            metadata.setSyncAttempt(1);
            metadata.setEsSyncMillis(null);
            //entityMetadataRepository.save(metadata);
           entityMetadataRepository.update(metadata.getMetaId(), metadata);
            sendToDLQ(entityEvent, e);
        }
    }

    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
        try {
            kafkaTemplate.send("dlq35", failedEvent.getEntity().getId(), failedEvent);
            System.out.println("Message sent to DLQ: " + failedEvent);
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
        }
    }

}