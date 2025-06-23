package com.Packages.dlq;

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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class DLQConsumerVersion {
    private static final int MAX_RETRIES = 5;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final EntityMongoRepository               mongoRepo;
    private final EntityElasticRepository             elasticRepo;
    private final EntityMetadataRepository            metaRepo;
    private final KafkaTemplate<String, EntityEventVersion> kafkaTemplate;

    @Autowired
    public DLQConsumerVersion(
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

    @KafkaListener(topics = "dlq110", groupId = "dlq-consumer-group")
    public void consumeDLQ(EntityEventVersion event) {
        EntityMetadataversion meta   = event.getEntityMetadataversion();
        String              metaId  = meta.getMetaId();
        int                 tries   = meta.getSyncAttempt();
        int                 nextTry = tries + 1;
        String              entityId= event.getEntityId();
        long                version = event.getVersion();
        String              op      = meta.getOperation();
        String              idx     = "entity";
        System.out.println(op);
        Entity entity = event.getEntity();
        if (entity == null) {
            return;
        }

        try {
            switch (op) {
                case "create" -> elasticRepo.createEntityWithVersion(
                        idx, entityId, entity, version);
                case "update" -> elasticRepo.updateEntityWithVersion(
                        idx, entityId, entity, version);
                case "delete" -> elasticRepo.deleteEntityWithVersion(
                        idx, entityId, version);
                default       -> { return; }
            }
            meta.setSyncAttempt(nextTry);
            meta.setEsStatus("success");
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setDlqReason(null);
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
                meta.setSyncAttempt(nextTry);
                meta.setEsStatus("failure");
                meta.setEsSyncMillis(null);
                meta.setDlqReason(reason);
                metaRepo.updateversion(metaId, meta);
                return;
            }
            scheduleRetry(event, meta, nextTry, reason);

        } catch (Exception ex) {
            scheduleRetry(event, meta, nextTry, ex.getMessage());
        }
    }

    private void scheduleRetry(EntityEventVersion event,
                               EntityMetadataversion meta,
                               int nextTry,
                               String reason) {
        meta.setSyncAttempt(nextTry);
        meta.setEsStatus("failure");
        meta.setEsSyncMillis(null);
        meta.setDlqReason(reason);
        metaRepo.updateversion(meta.getMetaId(), meta);

        if (nextTry > MAX_RETRIES) {
            return;
        }
        long delaySec = Math.min(30, 1L << (nextTry - 1));
        scheduler.schedule(() ->
                        kafkaTemplate.send("dlq110", event.getEntityId(), event),
                delaySec, TimeUnit.SECONDS
        );
    }
}
