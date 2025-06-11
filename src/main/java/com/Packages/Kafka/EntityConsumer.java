package com.Packages.Kafka;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Model.EntityMetadata;
import com.Packages.Repository.EntityElasticRepository;
import com.Packages.Repository.EntityMetadataRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    @KafkaListener(topics = "Entity10", groupId = "es-consumer-group")
    public void Consume(EntityEvent entityEvent){
        try {
            EntityDTO entityDTO = entityEvent.getEntityDTO();
            String operation = entityEvent.getOperation();
            String indexName= entityEvent.getIndex();
            String documentId = entityEvent.getId();
            switch (operation) {
                case "create":
                    entityElasticRepository.createEntity(
                            indexName,
                            Entity.fromDTO(entityDTO)
                    );
                    break;
                    case "update":
                    entityElasticRepository.updateEntity(
                            indexName,
                            documentId,
                            Entity.fromDTO(entityDTO),
                            entityDTO.getCreateTime()
                    );
                    break;
                    case "delete":
                    entityElasticRepository.deleteEntity(indexName, documentId);
                    break;
            }
            EntityMetadata entityMetadata = entityEvent.getMetadata();
            if (entityMetadata !=null){
            entityMetadata.setEsSyncMillis(System.currentTimeMillis());
            entityMetadata.setSyncAttempt(entityMetadata.getSyncAttempt() + 1);
            entityMetadata.setEsStatus("SUCCESS");
            entityMetadataRepository.save(entityMetadata);}
        }
        catch (Exception e) {
            System.err.println("Failed to save data in Elasticsearch"+e.getMessage());
            sendToDLQ(entityEvent, e);
            if (entityEvent.getMetadata() != null) {
                EntityMetadata metadata = entityEvent.getMetadata();
                metadata.setEsStatus("FAILURE");
                metadata.setDlqReason(e.getMessage());
                entityMetadataRepository.save(metadata);
            }
            throw new RuntimeException("Failed to sync to Elasticsearch", e);
        }
    }
    private void sendToDLQ(EntityEvent failedEvent, Exception e) {
        try {
            kafkaTemplate.send("dlq-entity10",failedEvent.getEntityDTO().getId(), failedEvent);
            System.out.println("Message sent to DLQ: " + failedEvent);
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
        }
    }
    }