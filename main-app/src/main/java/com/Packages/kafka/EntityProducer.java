package com.Packages.kafka;

import com.Packages.model.EntityEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EntityProducer {
    private static final String topic = "entity91";
    @Autowired
    private KafkaTemplate<String, EntityEvent> kafkaTemplate;
    public void sendToKafka(EntityEvent entityEvent) {
        kafkaTemplate.send(topic, entityEvent.getEntity().getId(), entityEvent);
    }
}