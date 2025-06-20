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
        }

        for (ChangeStreamDocument<Document> change : stream) {
            try {
                processChange(change, 0);
            } catch (Exception e) {

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
        EntityMetadata meta = metadataRepo.getById(metaId);
        try {
            if ("delete".equals(op)) {
                esRepo.deleteEntity("entity", id);
            }
            else if ("update".equals(op) || "replace".equals(op)) {
                if (!"delete".equals(meta.getOperation())) {
                    esRepo.updateEntity("entity", id, documentToEntity(post), null);
                } else {
                    saveToken(change.getResumeToken());
                    return;
                }
            }
            else if ("insert".equals(op)) {
                esRepo.createEntity("entity", documentToEntity(post));
            } else {
                return;
            }
            meta.setEsStatus("success");
            meta.setSyncAttempt(attempts + 1);
            meta.setEsSyncMillis(System.currentTimeMillis());
            meta.setDlqReason(null);
            metadataRepo.update(meta.getMetaId(), meta);
            saveToken(change.getResumeToken());
        } catch (Exception es) {
            handleException(es, change, meta, attempts);
        }
    }

    private void handleException(Exception ex,
                                   ChangeStreamDocument<Document> change,
                                   EntityMetadata meta,
                                   int attempts) {
        String reason;
        if (ex instanceof ElasticsearchException ee) {
            reason = ee.error().reason();
        } else {
            reason = ex.getMessage();
        }
        if (ex instanceof ElasticsearchException ee
                && ee.status() >= 400 && ee.status() < 500) {
            meta.setEsStatus("failure");
            meta.setDlqReason(reason);
            meta.setSyncAttempt(1);
            meta.setEsSyncMillis(null);
            metadataRepo.update(meta.getMetaId(), meta);
            return;
        }else if (attempts < MAX_RETRIES) {
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
