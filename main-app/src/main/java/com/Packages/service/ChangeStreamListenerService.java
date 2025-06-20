//package com.Packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.Packages.model.ChangeStreamState;
//import com.Packages.model.Entity;
//import com.Packages.model.EntityMetadata;
//import com.Packages.repositoryinterface.ChangeStreamStateRepository;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMetadataRepository;
//import com.mongodb.client.ChangeStreamIterable;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.mongodb.client.model.changestream.FullDocument;
//import org.bson.BsonDocument;
//import org.bson.Document;
//import org.springframework.stereotype.Service;
//
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import java.time.Instant;
//import java.util.Optional;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
//@Service
//public class ChangeStreamListenerService {
//    private static final int maxRetries = 5;
//    private static final String dbName   = "Datasync";
//    private static final String collName= "Entity";
//    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//    private final MongoClient               mongoClient;
//    private final EntityElasticRepository   esRepo;
//    private final EntityMetadataRepository  metadataRepo;
//    private final ChangeStreamStateRepository tokenRepo;
//    private volatile BsonDocument resumeToken;
//    private volatile Instant      lastUpdated;
//    public ChangeStreamListenerService(
//            MongoClient mongoClient,
//            EntityElasticRepository esRepo,
//            EntityMetadataRepository metadataRepo,
//            ChangeStreamStateRepository tokenRepo) {
//        this.mongoClient  = mongoClient;
//        this.esRepo       = esRepo;
//        this.metadataRepo = metadataRepo;
//        this.tokenRepo    = tokenRepo;
//        this.resumeToken  = loadToken();
//    }
//    @PostConstruct
//    public void autoStart() {
//        System.out.println("POST-CONSTRUCT triggered");
//        startListener();
//    }
//
//    @PreDestroy
//    public void shutdown() {
//        scheduler.shutdownNow();
//    }
//
//    public void startListener() {
//        scheduler.submit(this::listenLoop);
//        System.out.println("ChangeStream listener started");
//    }
//
//    private void listenLoop() {
//        MongoDatabase db   = mongoClient.getDatabase(dbName);
//        MongoCollection<Document> coll = db.getCollection(collName);
//        System.out.println("WATCH OPEN on db=" + dbName + " coll=" + collName);
//        ChangeStreamIterable<Document> stream = coll.watch()
//                .fullDocument(FullDocument.UPDATE_LOOKUP);
//        if (resumeToken != null) {
//            stream = stream.resumeAfter(resumeToken);
//            System.out.println("Resuming after token: " + resumeToken);
//        }
//        for (ChangeStreamDocument<Document> change : stream) {
//            System.out.println("CHANGE ARRIVED");
//            processChange(change, 0);
//        }
//    }
//
//    private void processWithRetry(ChangeStreamDocument<Document> change, int attempts) {
//        System.out.println("naman");
//        scheduler.execute(() -> {
//            try {
//                processChange(change, attempts);
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        });
//    }
//
//    private void processChange(ChangeStreamDocument<Document> change, int attempts) {
//        System.out.println("priyanka");
//        String op     = change.getOperationType().getValue();
//        String id     = change.getDocumentKey().getString("_id").getValue();
//        BsonDocument token = change.getResumeToken();
//        Document doc  = change.getFullDocument();
//        System.out.println("CS-EVENT op=" + op + ", id=" + id +
//                ", metadataId=" + doc.getString("metadataId"));
//        EntityMetadata meta = resolveMetadata(id, op, doc);
//        try {
//            switch (op) {
//                case "create":
//                    esRepo.createEntity("entity", documentToEntity(doc));
//                    break;
//                case "update":
//                    esRepo.updateEntity("entity", id, documentToEntity(doc), null);
//                    break;
//                case "delete":
//                    esRepo.deleteEntity("entity", id);
//                    break;
//                default:
//                    System.out.println("Unknown op: " + op);
//            }
//            meta.setEsStatus("success");
//            meta.setSyncAttempt(attempts + 1);
//            meta.setEsSyncMillis(System.currentTimeMillis());
//            meta.setDlqReason(null);
//            System.out.println("Attempt");
//            metadataRepo.update(meta.getMetaId(), meta);
//            System.out.println("successful");
//            saveToken(token);
//
//        } catch (ElasticsearchException ee) {
//            int status = ee.status();
//            String reason = ee.error() != null ? ee.error().reason() : ee.getMessage();
//            if (status >= 400 && status < 500) {
//                meta.setEsStatus("failure");
//                meta.setSyncAttempt(attempts + 1);
//                meta.setEsSyncMillis(null);
//                meta.setDlqReason(reason);
//                metadataRepo.update(meta.getMetaId(), meta);
//                saveToken(token);
//            } else if (attempts < maxRetries) {
//                scheduleRetry(change, attempts + 1);
//            } else {
//                meta.setEsStatus("failure");
//                meta.setSyncAttempt(attempts + 1);
//                meta.setEsSyncMillis(null);
//                meta.setDlqReason(reason);
//                metadataRepo.update(meta.getMetaId(), meta);
//                saveToken(token);
//            }
//        } catch (RuntimeException re) {
//            if (attempts < maxRetries) {
//                scheduleRetry(change, attempts + 1);
//            } else {
//                meta.setEsStatus("failure");
//                meta.setSyncAttempt(attempts + 1);
//                meta.setEsSyncMillis(null);
//                meta.setDlqReason(re.getMessage());
//                metadataRepo.update(meta.getMetaId(), meta);
//                saveToken(token);
//            }
//        }
//    }
//
//    private void scheduleRetry(ChangeStreamDocument<Document> change, int attempts) {
//        long backoffSeconds = Math.min((long) Math.pow(2, attempts), 10L);
//        scheduler.schedule(() -> processChange(change, attempts),
//                backoffSeconds, TimeUnit.MILLISECONDS);
//    }
//    private EntityMetadata resolveMetadata(String entityId, String op, Document fullDoc) {
//        if (!"delete".equals(op)) {
//            String metaId = fullDoc.getString("metadataId");
//            return Optional.ofNullable(metadataRepo.getById(metaId))
//                    .orElseThrow(() -> new IllegalStateException("Missing metadata " + metaId));
//        } else {
//            EntityMetadata meta = metadataRepo.findByEntityIdAndOperation(entityId, "delete");
//            if (meta == null) {
//                throw new IllegalStateException("No delete metadata for " + entityId);
//            }
//            return meta;
//        }
//    }
//    private void saveToken(BsonDocument token) {
//        ChangeStreamState state = new ChangeStreamState();
//        state.setId("mongoToEsSync");
//        state.setResumeToken(Document.parse(token.toJson()));
//        state.setLastUpdated(Instant.now());
//        tokenRepo.save(state);
//        this.resumeToken = token;
//        this.lastUpdated = state.getLastUpdated();
//    }
//    private BsonDocument loadToken() {
//        return tokenRepo.findById("mongoToEsSync")
//                .map(ChangeStreamState::getResumeToken)
//                .map(doc -> BsonDocument.parse(doc.toJson()))
//                .orElse(null);
//    }
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
//                .metadataId(d.getString("metadataId"))
//                .build();
//    }
//}
