package com.Packages.Kafka;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.Packages.DTO.EntityDTO;
import com.Packages.Model.Entity;
import com.Packages.Model.EntityEvent;
import com.Packages.Repository.EntityElasticRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EntityConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final EntityElasticRepository entityElasticRepository;

    @Autowired
    public EntityConsumer(EntityElasticRepository entityElasticRepository) {
        this.entityElasticRepository = entityElasticRepository;
    }
    @KafkaListener(topics = "Entity", groupId = "es-consumer-group")
    public void Consume(String message){
        try {
            EntityEvent entityEvent = objectMapper.readValue(message,EntityEvent.class);
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
