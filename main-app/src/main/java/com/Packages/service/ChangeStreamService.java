//package com.Packages.service;
//
//import com.Packages.model.ChangeStreamState;
//import com.Packages.repository.ChangeStreamStateRepository;
//import com.Packages.repository.EntityElasticRepository;
//import com.Packages.repository.EntityMongoRepository;
//import com.Packages.model.Entity;
//
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.ChangeStreamIterable;
//import com.mongodb.client.model.changestream.FullDocument;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//
//import org.bson.BsonDocument;
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import javax.annotation.PreDestroy;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
///**
//
//
//
//@Service
//public class ChangeStreamService {
//    private static final Logger log = LoggerFactory.getLogger(ChangeStreamService.class);
//
//    private final MongoClient mongoClient;
//    private final ChangeStreamStateRepository stateRepo;
//    private final EntityElasticRepository esRepo;
//    private final EntityMongoRepository mongoRepo;
//    private final String databaseName;
//    private final String collectionName;
//
//
//    private ExecutorService listenerPool;
//
//    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor();
//
//    private volatile BsonDocument resumeToken;
//    private volatile Instant lastUpdated;
//
//    public ChangeStreamService(
//            MongoClient mongoClient,
//            ChangeStreamStateRepository stateRepo,
//            EntityElasticRepository esRepo,
//            EntityMongoRepository mongoRepo,
//            @Value("${spring.data.mongodb.database}") String databaseName,
//            @Value("${yourapp.change-stream.collection:entity}") String collectionName
//    ) {
//        this.mongoClient = mongoClient;
//        this.stateRepo = stateRepo;
//        this.esRepo = esRepo;
//        this.mongoRepo = mongoRepo;
//        this.databaseName = databaseName;
//        this.collectionName = collectionName;
//        this.resumeToken = loadToken();
//    }
//
//
//    public void startListening() {
//        if (listenerPool != null && !listenerPool.isShutdown()) {
//            log.info("Change stream already running");
//            return;
//        }
//        listenerPool = Executors.newSingleThreadExecutor();
//        listenerPool.submit(this::listenLoop);
//        log.info("Change stream listener started");
//    }
//
//
//    @PreDestroy
//    public void stop() {
//        if (listenerPool != null) {
//            listenerPool.shutdownNow();
//            listenerPool = null;
//            log.info("Change stream listener stopped");
//        }
//        retryScheduler.shutdownNow();
//        log.info("Retry scheduler stopped");
//    }
//
//
//    public BsonDocument getResumeToken() {
//        return resumeToken;
//    }
//
//
//    public Instant getLastUpdated() {
//        return lastUpdated;
//    }
//
//
//    private void listenLoop() {
//        MongoDatabase db = mongoClient.getDatabase(databaseName);
//        MongoCollection<Document> coll = db.getCollection(collectionName);
//
//        ChangeStreamIterable<Document> stream = coll
//                .watch()
//                .fullDocument(FullDocument.UPDATE_LOOKUP);
//
//        if (resumeToken != null) {
//            stream = stream.resumeAfter(resumeToken);
//            log.info("Resuming after token: {}", resumeToken);
//        }
//
//        for (ChangeStreamDocument<Document> change : stream) {
//            try {
//                // schedule first attempt immediately
//                scheduleAttempt(change, 0);
//            } catch (Throwable t) {
//                log.error("Stream listener aborted on error", t);
//                break;
//            }
//        }
//    }
//
//
//
//
//
//
//    private void scheduleAttempt(ChangeStreamDocument<Document> change, int attempts) {
//        long delay = (attempts == 0 ? 0 : Math.min(attempts, 10));
//        retryScheduler.schedule(() -> {
//            BsonDocument token = change.getResumeToken();
//            Document doc = change.getFullDocument();
//            String op = change.getOperationType().getValue();
//            try {
//                switch (op) {
//                    case "insert":
//                        esRepo.createEntity(collectionName, toEntity(doc));
//                        // success → persist token
//                        saveToken(token);
//                        break;
//                    case "update":
//                        esRepo.updateEntity(
//                                collectionName,
//                                change.getDocumentKey().getString("_id").getValue(),
//                                toEntity(doc),
//                                null
//                        );
//                        saveToken(token);
//                        break;
//                    case "delete":
//                        esRepo.deleteEntity(
//                                collectionName,
//                                change.getDocumentKey().getString("_id").getValue()
//                        );
//                        saveToken(token);
//                        break;
//                    default:
//                        log.warn("Unsupported op {} – skipping", op);
//                        saveToken(token);
//                }
//            } catch (ElasticsearchException ee) {
//                int status = ee.status();
//                String reason = ee.error() != null ? ee.error().reason() : ee.getMessage();
//                if (status >= 400 && status < 500) {
//                    // invalid data → skip and save
//                    log.info("Skipping invalid data (status={}): {}", status, reason);
//                    saveToken(token);
//                } else {
//                    // transient ES error → retry
//                    log.warn("Transient ES error (status={}), retry #{}", status, attempts+1);
//                    scheduleAttempt(change, attempts+1);
//                }
//            } catch (RuntimeException re) {
//                // other runtime (ES-down simulation) → retry
//                log.warn("ES-down simulation, retry #{}: {}", attempts+1, re.getMessage());
//                scheduleAttempt(change, attempts+1);
//            }
//        }, delay, TimeUnit.SECONDS);
//    }
//
//
//    private void saveToken(BsonDocument token) {
//        ChangeStreamState state = new ChangeStreamState();
//        state.setId("mongoToEsSync");
//        state.setResumeToken(token);
//        state.setLastUpdated(Instant.now());
//        stateRepo.save(state);
//        this.resumeToken = token;
//        this.lastUpdated = state.getLastUpdated();
//    }
//
//
//    private BsonDocument loadToken() {
//        return stateRepo.findById("mongoToEsSync")
//                .map(ChangeStreamState::getResumeToken)
//                .orElse(null);
//    }
//
//
//    private Entity toEntity(Document d) {
//        String id = d.getString("_id");
//        String name = d.getString("name");
//        if (id == null || name == null) {
//            throw new RuntimeException("Missing _id or name in doc: " + d);
//        }
//        LocalDateTime createTime = LocalDateTime.ofInstant(
//                d.getDate("createTime").toInstant(), ZoneId.systemDefault());
//        LocalDateTime modifiedTime = LocalDateTime.ofInstant(
//                d.getDate("modifiedTime").toInstant(), ZoneId.systemDefault());
//        return Entity.builder()
//                .id(id)
//                .name(name)
//                .createTime(createTime)
//                .modifiedTime(modifiedTime)
//                .build();
//    }
//}
