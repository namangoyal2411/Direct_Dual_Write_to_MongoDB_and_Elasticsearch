package com.Packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.Packages.model.ChangeStreamState;
import com.Packages.model.Entity;
import com.Packages.model.EntityMetadata;
import com.Packages.repositoryinterface.ChangeStreamStateRepository;
import com.Packages.repository.EntityElasticRepository;
import com.Packages.repository.EntityMetadataRepository;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@Profile("stream")
public class ChangeStreamListenerService {
    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
    private static final int MAX_RETRIES = 5;
    private static final String DB_NAME = "Datasync";
    private static final String COLL_NAME = "Entity";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final MongoClient mongoClient;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metadataRepo;
    private final ChangeStreamStateRepository tokenRepo;
    private volatile BsonDocument resumeToken;

    public ChangeStreamListenerService(
            MongoClient mongoClient,
            EntityElasticRepository esRepo,
            EntityMetadataRepository metadataRepo,
            ChangeStreamStateRepository tokenRepo) {
        this.mongoClient = mongoClient;
        this.esRepo = esRepo;
        this.metadataRepo = metadataRepo;
        this.tokenRepo = tokenRepo;
        this.resumeToken = loadToken();
    }

    @PostConstruct
    public void autoStart() {
        log.info("Starting ChangeStreamListener");
        startListener();
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }

    public void startListener() {
        scheduler.submit(this::listenLoop);
    }

    private void listenLoop() {
        MongoDatabase db = mongoClient.getDatabase(DB_NAME);
        MongoCollection<Document> coll = db.getCollection(COLL_NAME);

        ChangeStreamIterable<Document> stream = coll.watch()
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
        if (resumeToken != null) {
            stream = stream.resumeAfter(resumeToken);
            log.info("Resuming change stream after token");
        }

        for (ChangeStreamDocument<Document> change : stream) {
            try {
                processChange(change, 0);
            } catch (Exception e) {
                log.error("Failed to process change; continuing", e);
            }
        }
    }

    private void processChange(ChangeStreamDocument<Document> change, int attempts) {
        String op = change.getOperationType().getValue();
        String id = change.getDocumentKey().getString("_id").getValue();
        Document post   = change.getFullDocument();
        Document pre    = change.getFullDocumentBeforeChange();

        String metaId;
        if ("delete".equals(op)) {
            metaId = (pre != null) ? pre.getString("metadataId") : null;
        } else {
            metaId = (post != null) ? post.getString("metadataId") : null;
        }
        log.info("CS-EVENT op={}, id={}, metadataId={}", op, id, metaId);
        EntityMetadata meta = metadataRepo.getById(metaId);

        boolean touchingDeleteRow =
                ("update".equals(op) || "replace".equals(op))
                        && "delete".equals(meta.getOperation());

        if ( touchingDeleteRow) {
            saveToken(change.getResumeToken());
            return;
        }
//        if ("delete".equals(op)) {
        // little bit doubt in this part
//            System.out.println(meta.getEsStatus());
//        }

        try {
            if ("delete".equals(op)) {
                esRepo.deleteEntity("entity", id);
            }
            else if ("update".equals(op) || "replace".equals(op)) {
                if (!"delete".equals(meta.getOperation())) {
                    esRepo.updateEntity("entity", id, documentToEntity(post), null);
                } else {
                    log.info("Skipping synthetic patch for delete-meta {}", meta.getMetaId());
                    saveToken(change.getResumeToken());
                    return;
                }
            }
            else if ("insert".equals(op)) {
                esRepo.createEntity("entity", documentToEntity(post));
            } else {
                log.warn("Skipping unsupported op {}", op);
                return;
            }
            meta.setEsStatus("success");
            meta.setSyncAttempt(attempts + 1);
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setDlqReason(null);
            metadataRepo.update(meta.getMetaId(), meta);
            saveToken(change.getResumeToken());
        } catch (ElasticsearchException ee) {
            handleEsException(ee, change, meta, attempts);
        } catch (RuntimeException re) {
            handleRuntimeException(re, change, meta, attempts);
        }
    }

    private void handleEsException(ElasticsearchException ee,
                                   ChangeStreamDocument<Document> change,
                                   EntityMetadata meta,
                                   int attempts) {
        int status = ee.status();
        String reason = ee.error() != null ? ee.error().reason() : ee.getMessage();
        if (status >= 400 && status < 500) {
            meta.setEsStatus("failure");
            meta.setSyncAttempt(attempts + 1);
            meta.setEsSyncMillis(null);
            meta.setDlqReason(reason);
            metadataRepo.update(meta.getMetaId(), meta);
            saveToken(change.getResumeToken());
        } else if (attempts < MAX_RETRIES) {
            scheduleRetry(change, attempts + 1);
        } else {
            meta.setEsStatus("failure");
            meta.setSyncAttempt(attempts + 1);
            meta.setEsSyncMillis(null);
            meta.setDlqReason(reason);
            metadataRepo.update(meta.getMetaId(), meta);
            saveToken(change.getResumeToken());
        }
    }

    private void handleRuntimeException(RuntimeException re,
                                        ChangeStreamDocument<Document> change,
                                        EntityMetadata meta,
                                        int attempts) {
        if (attempts < MAX_RETRIES) {
            scheduleRetry(change, attempts + 1);
        } else {
            meta.setEsStatus("failure");
            meta.setSyncAttempt(attempts + 1);
            meta.setEsSyncMillis(null);
            meta.setDlqReason(re.getMessage());
            metadataRepo.update(meta.getMetaId(), meta);
            saveToken(change.getResumeToken());
        }
    }

    private void scheduleRetry(ChangeStreamDocument<Document> change, int attempts) {
        long delaySec = Math.min((1L << attempts), 10L);
        scheduler.schedule(() -> processChange(change, attempts), delaySec, TimeUnit.MILLISECONDS);
    }

    private void saveToken(BsonDocument token) {
        ChangeStreamState state = new ChangeStreamState();
        state.setId("mongoToEsSync");
        state.setResumeToken(Document.parse(token.toJson()));
        state.setLastUpdated(Instant.now());
        tokenRepo.save(state);
        this.resumeToken = token;
    }

    private BsonDocument loadToken() {
        return tokenRepo.findById("mongoToEsSync")
                .map(ChangeStreamState::getResumeToken)
                .map(doc -> BsonDocument.parse(doc.toJson()))
                .orElse(null);
    }

    private Entity documentToEntity(Document d) {
        return Entity.builder()
                .id(d.getString("_id"))
                .name(d.getString("name"))
                .createTime(d.getDate("createTime") == null ? null :
                        d.getDate("createTime").toInstant()
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime())
                .modifiedTime(d.getDate("modifiedTime") == null ? null :
                        d.getDate("modifiedTime").toInstant()
                                .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime())
                .metadataId(d.getString("metadataId"))
                .build();
    }
}
