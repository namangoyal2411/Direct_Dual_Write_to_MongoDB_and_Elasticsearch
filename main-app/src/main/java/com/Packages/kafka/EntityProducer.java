package com.Packages.kafka;

import com.Packages.model.EntityEvent;
import com.Packages.model.EntityEventVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EntityProducer {
    private static final String topic = "entity108";
    @Autowired
    private KafkaTemplate<String, EntityEvent> kafkaTemplate;
    private KafkaTemplate<String, EntityEventVersion> kafkaTemplateVersion;
    public void sendToKafka(EntityEvent entityEvent) {
        kafkaTemplate.send(topic, entityEvent.getEntity().getId(), entityEvent);
    }
}