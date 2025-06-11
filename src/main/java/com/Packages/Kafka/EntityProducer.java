package com.Packages.Kafka;

import com.Packages.DTO.EntityDTO;

import com.Packages.Model.EntityEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class EntityProducer {
    private static final String topic = "Entity9";
    @Autowired
    private KafkaTemplate<String, EntityEvent> kafkaTemplate;
    public void sendToKafka(EntityEvent entityEvent) {
        kafkaTemplate.send(topic, entityEvent.getEntityDTO().getId(), entityEvent);
    }
}