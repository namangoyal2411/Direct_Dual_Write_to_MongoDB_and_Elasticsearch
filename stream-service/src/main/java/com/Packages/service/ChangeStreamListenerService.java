//package com.Packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.Packages.model.ChangeStreamState;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityMetadata;
//import com.Packages.model.EntityMetadataversion;
//import com.Packages.repositoryinterface.ChangeStreamStateRepository;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import com.mongodb.client.ChangeStreamIterable;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.mongodb.client.model.changestream.FullDocument;
//import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
//import com.mongodb.client.model.changestream.OperationType;
//import org.bson.BsonDocument;
//import org.bson.BsonTimestamp;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Profile;
//import org.springframework.stereotype.Service;
//
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import java.time.Instant;
//import java.util.UUID;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
//@Service
//@Profile("stream")
//public class ChangeStreamListenerService {
//    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
//    private static final int MAX_RETRIES = 5;
//    private static final String DB_NAME = "Datasync";
//    private static final String COLL_NAME = "entitychangestream";
//    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
//    private final MongoClient mongoClient;
//    private final EntityElasticRepository esRepo;
//    private final EntityMetadataRepository metadataRepo;
//    private final ChangeStreamStateRepository tokenRepo;
//    private volatile BsonDocument resumeToken;
//
//    public ChangeStreamListenerService(
//            MongoClient mongoClient,
//            EntityElasticRepository esRepo,
//            EntityMetadataRepository metadataRepo,
//            ChangeStreamStateRepository tokenRepo) {
//        this.mongoClient = mongoClient;
//        this.esRepo = esRepo;
//        this.metadataRepo = metadataRepo;
//        this.tokenRepo = tokenRepo;
//        this.resumeToken = loadToken();
//    }
//
//    @PostConstruct
//    public void autoStart() {
//        log.info("Starting ChangeStreamListener");
//        startListener();
//    }
//
//    @PreDestroy
//    public void shutdown() {
//        scheduler.shutdown();
//    }
//
//    public void startListener() {
//        scheduler.submit(this::listenLoop);
//    }
//
//    private void listenLoop() {
//        MongoCollection<Document> coll = mongoClient
//                .getDatabase(DB_NAME)
//                .getCollection(COLL_NAME);
//
//        ChangeStreamIterable<Document> stream;
//
//        if (resumeToken != null) {
//            // resume from last‐saved token
//            stream = coll.watch()
//                    .resumeAfter(resumeToken)
//                    .fullDocument(FullDocument.UPDATE_LOOKUP)
//                    .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
//            log.info("Resuming change stream after saved token");
//        } else {
//            // start only from “now”
//            Document hello = mongoClient
//                    .getDatabase("admin")
//                    .runCommand(new Document("hello", 1));
//            BsonTimestamp nowTs = hello.get("operationTime", BsonTimestamp.class);
//
//            stream = coll.watch()
//                    .startAtOperationTime(nowTs)
//                    .fullDocument(FullDocument.UPDATE_LOOKUP)
//                    .fullDocumentBeforeChange(FullDocumentBeforeChange.WHEN_AVAILABLE);
//            log.info("Starting change stream at operationTime={}", nowTs);
//        }
//
//        for (ChangeStreamDocument<Document> change : stream) {
//            try {
//                processChange(change, 0);
//            } catch (Exception e) {
//                log.error("Error processing change", e);
//            }
//        }
//    }
//
//    private void processChange(ChangeStreamDocument<Document> change, int attempts) {
//        if (change.getOperationType() == null) {
//            return;
//        }
//        String op = change.getOperationType().getValue();
//        Document post = change.getFullDocument();
//        Document pre  = change.getFullDocumentBeforeChange();
//        Entity entity = documentToEntity(post != null ? post : pre);
//        String id = entity.getId();
//        long  version = entity.getVersion();
//        log.info(op);
//        if ("delete".equals(op)){
//            version++;}
//        String metaId = String.format("%s-%s%d", id, op, version);
//        EntityMetadata meta = EntityMetadata.builder()
//                .metaId(metaId)
//                .entityId(id)
//                .approach("Change Stream")
//                .operation(op)
//                .operationSeq(version)
//                .mongoWriteMillis(System.currentTimeMillis())
//                .esSyncMillis(null)
//                .firstFailureTime(null)
//                .syncAttempt(0)
//                .mongoStatus("success")
//                .esStatus("pending")
//                .dlqReason(null)
//                .build();
//        try {
//            switch (op) {
//                case "delete" -> esRepo.deleteEntity("entitychangestream", id);
//                case "update", "replace" -> esRepo.updateEntity("entitychangestream", id, documentToEntity(post), null);
//                case "insert" -> esRepo.createEntity("entitychangestream", documentToEntity(post));
//                default -> {
//                    return;
//                }
//            }
//            meta.setEsStatus("success");
//            meta.setSyncAttempt(attempts + 1);
//            meta.setEsSyncMillis(System.currentTimeMillis());
//            meta.setDlqReason(null);
//            saveToken(change.getResumeToken());
//        } catch (Exception es) {
//            if (meta.getFirstFailureTime() == null) {
//                meta.setFirstFailureTime(System.currentTimeMillis());
//            }
//            handleException(es, change, meta, attempts);
//        } finally {
//            metadataRepo.save(meta);
//        }
//    }
//
//    private void handleException(Exception ex,
//                                   ChangeStreamDocument<Document> change,
//                                   EntityMetadata meta,
//                                   int attempts) {
//        String reason;
//        if (ex instanceof ElasticsearchException ee) {
//            reason = ee.error().reason();
//        } else {
//            reason = ex.getMessage();
//        }
//        if (ex instanceof ElasticsearchException ee
//                && ee.status() >= 400 && ee.status() < 500) {
//            meta.setEsStatus("failure");
//            meta.setDlqReason(reason);
//            meta.setSyncAttempt(1);
//            meta.setEsSyncMillis(null);
//            saveToken(change.getResumeToken());
//            return;
//        }else if (attempts < MAX_RETRIES) {
//            scheduleRetry(change, attempts + 1);
//        }
//            meta.setEsStatus("failure");
//            meta.setSyncAttempt(attempts + 1);
//            meta.setEsSyncMillis(null);
//            meta.setDlqReason(reason);
//            saveToken(change.getResumeToken());
//
//    }
//
//
//    private void scheduleRetry(ChangeStreamDocument<Document> change, int attempts) {
//        long delaySec = Math.min((1L << attempts), 10L);
//        scheduler.schedule(() -> processChange(change, attempts), delaySec, TimeUnit.MILLISECONDS);
//    }
//
//    private void saveToken(BsonDocument token) {
//        ChangeStreamState state = new ChangeStreamState();
//        state.setId("mongoToEsSync");
//        state.setResumeToken(Document.parse(token.toJson()));
//        state.setLastUpdated(Instant.now());
//        tokenRepo.save(state);
//        this.resumeToken = token;
//    }
//
//    private BsonDocument loadToken() {
//        return tokenRepo.findById("mongoToEsSync")
//                .map(ChangeStreamState::getResumeToken)
//                .map(doc -> BsonDocument.parse(doc.toJson()))
//                .orElse(null);
//    }
//
//    private Entity documentToEntity(Document d) {
//        return Entity.builder()
//                .id(d.getString("_id"))
//                .name(d.getString("name"))
//                .createTime(d.getDate("createTime") == null ? null :
//                        d.getDate("createTime").toInstant()
//                                .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime())
//                .modifiedTime(d.getDate("modifiedTime") == null ? null :
//                        d.getDate("modifiedTime").toInstant()
//                                .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime())
//                .version(d.getLong("version"))
//                .build();
//    }
//}
