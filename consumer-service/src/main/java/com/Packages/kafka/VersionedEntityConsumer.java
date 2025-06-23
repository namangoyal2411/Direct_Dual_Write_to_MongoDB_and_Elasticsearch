package com.Packages.kafka;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.model.Entity;
import com.Packages.model.EntityEventVersion;
import com.Packages.model.EntityMetadataversion;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.Packages.repository.EntityMongoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class VersionedEntityConsumer {
    private final EntityMongoRepository                     mongoRepo;
    private final EntityElasticRepository                   elasticRepo;
    private final EntityMetadataRepository                  metaRepo;
    private final KafkaTemplate<String, EntityEventVersion> kafkaTemplate;
    @Autowired
    public VersionedEntityConsumer(
            EntityMongoRepository mongoRepo,
            EntityElasticRepository elasticRepo,
            EntityMetadataRepository metaRepo,
            @Qualifier("entityEventVersionKafkaTemplate")
            KafkaTemplate<String, EntityEventVersion> kafkaTemplate
    ) {
        this.mongoRepo      = mongoRepo;
        this.elasticRepo    = elasticRepo;
        this.metaRepo       = metaRepo;
        this.kafkaTemplate  = kafkaTemplate;
    }
    @KafkaListener(topics = "entity108", groupId = "es-consumer-group")
    public void onEntityEvent(EntityEventVersion event) {
        EntityMetadataversion meta    = event.getEntityMetadataversion();
        String                 metaId  = meta.getMetaId();
        String               entityId  = meta.getEntityId();
        String               operation = meta.getOperation();
        long                 version   = event.getVersion();
        String               indexName = "entity";
        System.out.println(operation);
        try {
            Entity entity = event.getEntity();
            switch (operation) {
                case "create" -> elasticRepo.createEntityWithVersion(
                        indexName, entityId, entity, version);
                case "update" -> elasticRepo.updateEntityWithVersion(
                        indexName, entityId, entity, version);
                case "delete" -> elasticRepo.deleteEntityWithVersion(
                        indexName, entityId, version);
                default -> {
                    return;
                }
            }
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            metaRepo.updateversion(metaId, meta);

        } catch (ElasticsearchException ee) {
            int status = ee.status();
            String reason = ee.error().reason();
            if (status == 409) {
                meta.setEsStatus("failure");
                meta.setDlqReason("Stale");
                return;
            }
            if (status >= 400 && status < 500) {
                meta.setEsStatus("failure");
                meta.setDlqReason(reason);
                meta.setSyncAttempt(meta.getSyncAttempt() + 1);
                meta.setEsSyncMillis(null);
                metaRepo.updateversion(metaId, meta);
                return;
            }
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(metaId, meta);
            kafkaTemplate.send("dlq108", entityId, event);

        } catch (IOException | RuntimeException ex) {
            String reason = ex.getMessage();
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(meta.getSyncAttempt() + 1);
            meta.setEsSyncMillis(null);
            metaRepo.updateversion(metaId, meta);
            kafkaTemplate.send("dlq108", entityId, event);
        }
    }
}
