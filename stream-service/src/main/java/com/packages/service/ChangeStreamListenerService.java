//
//package com.packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.mongodb.client.ChangeStreamIterable;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.mongodb.client.model.changestream.FullDocument;
//import com.packages.model.ChangeStreamState;
//import com.packages.model.Entity;
//import com.packages.model.EntityMetadata;
//import com.packages.repository.ChangeStreamStateRepository;
//import com.packages.repository.EntityElasticRepository;
//import com.packages.repository.EntityMetadataRepository;
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import org.bson.BsonDocument;
//import org.bson.BsonTimestamp;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Profile;
//import org.springframework.stereotype.Service;
//
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.ScheduledFuture;
//import java.util.concurrent.ThreadLocalRandom;
//import java.util.concurrent.TimeUnit;
//
//@Service
//@Profile("stream")
//public class ChangeStreamListenerService {
//    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
//    private static final int MAX_RETRIES = 5;
//    private static int counter = 0;
//    private static final String DB_NAME = "Datasync";
//    private static final String COLL_NAME = "Entity";
//
//    private final ExecutorService listener = Executors.newSingleThreadExecutor();
//    private final ExecutorService workers = Executors.newFixedThreadPool(10);
//    private final ScheduledExecutorService retryer = Executors.newScheduledThreadPool(4);
//
//    private final MongoClient mongoClient;
//    private final EntityElasticRepository esRepo;
//    private final EntityMetadataRepository metadataRepo;
//    private final ChangeStreamStateRepository tokenRepo;
//    private volatile BsonDocument resumeToken;
//    private final EntityMetadataService entityMetadataService;
//
//    @Autowired
//    public ChangeStreamListenerService(
//            MongoClient mongoClient,
//            EntityElasticRepository esRepo,
//            EntityMetadataRepository metadataRepo,
//            ChangeStreamStateRepository tokenRepo,
//            EntityMetadataService entityMetadataService) {
//        this.mongoClient = mongoClient;
//        this.esRepo = esRepo;
//        this.metadataRepo = metadataRepo;
//        this.tokenRepo = tokenRepo;
//        this.resumeToken = loadToken();
//        this.entityMetadataService = entityMetadataService;
//    }
//
//    @PostConstruct
//    public void start() {
//        listener.submit(this::listen);
//    }
//
//    @PreDestroy
//    public void stop() {
//        listener.shutdownNow();
//        workers.shutdown();
//        retryer.shutdown();
//    }
//
//    private void listen() {
//        var database = mongoClient.getDatabase(DB_NAME);
//        var coll = database.getCollection(COLL_NAME);
//
//        ChangeStreamIterable<Document> stream = (resumeToken != null)
//                ? coll.watch()
//                .resumeAfter(resumeToken)
//                .fullDocument(FullDocument.UPDATE_LOOKUP)
//                : coll.watch()
//                .startAtOperationTime(getCurrentTimestamp())
//                .fullDocument(FullDocument.UPDATE_LOOKUP);
//
//        List<ChangeStreamDocument<Document>> buffer = new ArrayList<>();
//        stream.forEach(change -> {
//           // log.info("Received entity: {}", counter++);
//            workers.submit(() -> processChange(change, 0));
//        });
//    }
//    public void processChange(ChangeStreamDocument<Document> change, int attempt) {
//       // log.info("Checking attempt {}", attempt);
//        String op = change.getOperationType().getValue();
//        Entity entity = toEntity(change);
//        long mongoTs = change.getClusterTime().getTime() * 1000L;
//
//        try {
//            switch (op) {
//                case "insert" -> esRepo.createEntity("entity", entity);
//                case "update", "replace" -> esRepo.updateEntity("entity", entity.getId(), entity);
//                default -> {
//                }
//            }
//            long esTs = System.currentTimeMillis();
//            if (attempt==0) {
//                entityMetadataService.createEntityMetadata(
//                        entity,
//                        op,
//                        "success",
//                        esTs,
//                        mongoTs,
//                        null
//                );
//            }
//            else {
//                String metaId = entity.getId() + "-" + op + "-" + entity.getVersion();
//                entityMetadataService.updateEntityMetadata(metaId,"success",System.currentTimeMillis(),null);}
//            saveToken(change.getResumeToken());
//        } catch (Exception ex) {
//            boolean isInvalidData = false;
//            String reason = ex.getMessage();
//
//            if (ex instanceof ElasticsearchException ee) {
//                if (ee.error() != null) {
//                    reason = ee.error().reason();
//                }
//                isInvalidData = (ee.status() == 400);
//            }
//            if (attempt==0) {
//                entityMetadataService.createEntityMetadata(
//                        entity,
//                        op,
//                        "failure",
//                        null,
//                        mongoTs,
//                        reason
//                );
//            }
//            else if (attempt>=MAX_RETRIES) {
//                String metaId = entity.getId() + "-" + op + "-" + entity.getVersion();
//                entityMetadataService.updateEntityMetadata(metaId,"failure",null,reason);
//                saveToken(change.getResumeToken());
//            }
//            if (!isInvalidData && attempt < MAX_RETRIES) {
//                long backoff = Math.min((1L << attempt) * 1000, 30000);
//                long jitter = ThreadLocalRandom.current().nextLong((long) (backoff * 0.8), (long) (backoff * 1.2));
//                log.warn("Scheduling retry #{} for entity {} in {} ms", attempt + 1, entity.getId(), jitter);
//                retryer.schedule(() -> processChange(change, attempt + 1), jitter, TimeUnit.MILLISECONDS);
//            }
//        }
//    }
//
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
//    private BsonTimestamp getCurrentTimestamp() {
//        Document hello = mongoClient.getDatabase("admin")
//                .runCommand(new Document("hello", 1));
//        return hello.get("operationTime", BsonTimestamp.class);
//    }
//
//    private Entity toEntity(ChangeStreamDocument<Document> change) {
//        Document doc = change.getFullDocument() != null
//                ? change.getFullDocument()
//                : change.getFullDocumentBeforeChange();
//        ObjectId oid = doc.getObjectId("_id");
//        String id = (oid != null ? oid.toHexString() : null);
//        return Entity.builder()
//                .id(id)
//                .name(doc.getString("name"))
//                .createTime(toLDT(doc.getDate("createTime")))
//                .modifiedTime(toLDT(doc.getDate("modifiedTime")))
//                .version(doc.getLong("version"))
//                .isDeleted(doc.getBoolean("isDeleted"))
//                .build();
//    }
//
//    private static LocalDateTime toLDT(java.util.Date d) {
//        return d == null
//                ? null
//                : d.toInstant()
//                .atZone(java.time.ZoneId.systemDefault())
//                .toLocalDateTime();
//    }
//}
//package com.packages.service;
//
//import co.elastic.clients.elasticsearch._types.ElasticsearchException;
//import com.mongodb.client.ChangeStreamIterable;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.model.changestream.ChangeStreamDocument;
//import com.mongodb.client.model.changestream.FullDocument;
//import com.packages.model.ChangeStreamState;
//import com.packages.model.Entity;
//import com.packages.repository.ChangeStreamStateRepository;
//import com.packages.repository.EntityElasticRepository;
//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
//import org.bson.BsonDocument;
//import org.bson.BsonTimestamp;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.context.annotation.Profile;
//import org.springframework.stereotype.Service;
//
//import java.io.IOException;
//import java.time.Instant;
//import java.time.LocalDateTime;
//import java.time.ZoneId;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.ThreadLocalRandom;
//import java.util.concurrent.TimeUnit;

//@Service
//@Profile("stream")
//public class ChangeStreamListenerService {
//    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
//    private static final int MAX_RETRIES = 5;
//    private static final String DB_NAME = "Datasync";
//    private static final String COLL_NAME = "Entity";
//
//    private final ExecutorService listener = Executors.newSingleThreadExecutor();
//    private final MongoClient mongoClient;
//    private final EntityElasticRepository esRepo;
//    private final ChangeStreamStateRepository tokenRepo;
//    private volatile BsonDocument resumeToken;
//
//    public ChangeStreamListenerService(
//            MongoClient mongoClient,
//            EntityElasticRepository esRepo,
//            ChangeStreamStateRepository tokenRepo
//    ) {
//        this.mongoClient = mongoClient;
//        this.esRepo = esRepo;
//        this.tokenRepo = tokenRepo;
//        this.resumeToken = loadToken();
//    }
//
//    @PostConstruct
//    public void start() {
//        listener.submit(this::listen);
//    }
//
//    @PreDestroy
//    public void stop() {
//        listener.shutdownNow();
//    }
//
//    private void listen() {
//        var database = mongoClient.getDatabase(DB_NAME);
//        var coll = database.getCollection(COLL_NAME);
//
//        ChangeStreamIterable<Document> stream = (resumeToken != null)
//                ? coll.watch().resumeAfter(resumeToken).fullDocument(FullDocument.UPDATE_LOOKUP)
//                : coll.watch().startAtOperationTime(getCurrentTimestamp()).fullDocument(FullDocument.UPDATE_LOOKUP);
//
//        stream.forEach(change -> {
//            BsonDocument token = change.getResumeToken();
//            for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
//                try {
//                    processChange(change);
//                    saveToken(token);
//                    break;
//                } catch (Exception ex) {
//                    boolean bad = ex instanceof ElasticsearchException ee && ee.error() != null && ee.status() == 400;
//                    if (bad || attempt == MAX_RETRIES) {
//                        saveToken(token);
//                        break;
//                    }
//                    long backoff = Math.min((1L << (attempt - 1)) * 1000L, 30000L);
//                    long jitter = ThreadLocalRandom.current().nextLong((long)(backoff*0.8), (long)(backoff*1.2));
//                    sleep(jitter);
//                }
//            }
//        });
//    }
//
//    private void processChange(ChangeStreamDocument<Document> change) throws IOException {
//        Entity e = toEntity(change);
//        String op = change.getOperationType().getValue();
//        switch (op) {
//            case "insert"  -> esRepo.createEntity("entity", e);
//            case "update", "replace" -> esRepo.updateEntity("entity", e.getId(), e);
//            default -> {}
//        }
//    }
//
//    private BsonDocument loadToken() {
//        return tokenRepo.findById("mongoToEsSync")
//                .map(ChangeStreamState::getResumeToken)
//                .orElse(null);
//    }
//
//    private void saveToken(BsonDocument token) {
//        tokenRepo.save(new ChangeStreamState("mongoToEsSync", token, Instant.now()));
//        resumeToken = token;
//    }
//
//    private BsonTimestamp getCurrentTimestamp() {
//        Document hello = mongoClient.getDatabase("admin").runCommand(new Document("hello", 1));
//        return hello.get("operationTime", BsonTimestamp.class);
//    }
//
//    private void sleep(long ms) {
//        try { TimeUnit.MILLISECONDS.sleep(ms); }
//        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
//    }
//
//    private Entity toEntity(ChangeStreamDocument<Document> change) {
//        Document d = change.getFullDocument();
//        ObjectId oid = d.getObjectId("_id");
//        return Entity.builder()
//                .id(oid != null ? oid.toHexString() : null)
//                .name(d.getString("name"))
//                .createTime(LocalDateTime.ofInstant(d.getDate("createTime").toInstant(), ZoneId.systemDefault()))
//                .modifiedTime(LocalDateTime.ofInstant(d.getDate("modifiedTime").toInstant(), ZoneId.systemDefault()))
//                .version(d.getLong("version"))
//                .isDeleted(d.getBoolean("isDeleted"))
//                .build();
//    }
//}
package com.packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.packages.model.ChangeStreamState;
import com.packages.model.Entity;
import com.packages.repository.ChangeStreamStateRepository;
import com.packages.repository.EntityElasticRepository;
import com.packages.service.EntityMetadataService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
@Profile("stream")
public class ChangeStreamListenerService {
    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
    private static final int MAX_RETRIES    = 5;
    private static final int WORKER_COUNT   = 1000;
    private static final int QUEUE_CAPACITY = 8;
    private static final String DB_NAME    = "Datasync";
    private static final String COLL_NAME  = "Entity";

    private final MongoClient                  mongoClient;
    private final EntityElasticRepository      esRepo;
    private final ChangeStreamStateRepository  tokenRepo;
    private final EntityMetadataService        metadataService;
    private volatile BsonDocument              resumeToken;

    private final BlockingQueue<ChangeStreamDocument<Document>> queue =
            new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    private final ExecutorService listener = Executors.newSingleThreadExecutor();
    private final ExecutorService workers  = Executors.newFixedThreadPool(WORKER_COUNT);

    public ChangeStreamListenerService(
            MongoClient mongoClient,
            EntityElasticRepository esRepo,
            ChangeStreamStateRepository tokenRepo,
            EntityMetadataService metadataService
    ) {
        this.mongoClient     = mongoClient;
        this.esRepo          = esRepo;
        this.tokenRepo       = tokenRepo;
        this.metadataService = metadataService;
        this.resumeToken     = loadToken();
    }

    @PostConstruct
    public void start() {
        listener.submit(this::produce);
        for (int i = 0; i < WORKER_COUNT; i++) {
            workers.submit(this::consume);
        }
    }

    @PreDestroy
    public void stop() {
        listener.shutdownNow();
        workers.shutdownNow();
    }

    private void produce() {
        var coll = mongoClient.getDatabase(DB_NAME).getCollection(COLL_NAME);
        ChangeStreamIterable<Document> stream = (resumeToken != null)
                ? coll.watch().resumeAfter(resumeToken).fullDocument(FullDocument.UPDATE_LOOKUP)
                : coll.watch().startAtOperationTime(getCurrentTimestamp()).fullDocument(FullDocument.UPDATE_LOOKUP);
        try (MongoCursor<ChangeStreamDocument<Document>> cursor = stream.iterator()) {
            while (!Thread.currentThread().isInterrupted() && cursor.hasNext()) {
                ChangeStreamDocument<Document> change = cursor.next();
                queue.put(change);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void consume() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                ChangeStreamDocument<Document> change = queue.take();
                BsonDocument token = change.getResumeToken();
                Entity entity = toEntity(change);
                String op = change.getOperationType().getValue();
                long mongoTs = change.getClusterTime().getTime() * 1000L;
                boolean done = false;
                for (int attempt = 1; attempt <= MAX_RETRIES && !done; attempt++) {
                    try {
                        switch (op) {
                            case "insert"  -> esRepo.createEntity("entity", entity);
                            case "update", "replace" -> esRepo.updateEntity("entity", entity.getId(), entity);
                            default -> {}
                        }
                        long esTs = System.currentTimeMillis();
                        if (attempt==1)
                    metadataService.createEntityMetadata(entity, op, "success", esTs, mongoTs, null);
                        else metadataService.updateEntityMetadata(entity.getId() + "-" + op + "-" + entity.getVersion(),
                                "success",
                                System.currentTimeMillis(),
                               null);
saveToken(token);
                        done = true;
                    } catch (Exception ex) {
                        boolean bad = ex instanceof ElasticsearchException ee && ee.error() != null && ee.status() == 400;
                        if (bad) {
                            metadataService.createEntityMetadata(entity, op, "failure", null, mongoTs, ex.getMessage());
                            saveToken(token);
                            done = true;
                        } else if (attempt == MAX_RETRIES) {
                            metadataService.updateEntityMetadata(
                                    entity.getId() + "-" + op + "-" + entity.getVersion(),
                                    "failure",
                                    null,
                                    ex.getMessage()
                            );
                            saveToken(token);
                            done = true;
                        } else {
                            if (attempt==1)
                           metadataService.createEntityMetadata(entity, op, "failure", null, mongoTs, ex.getMessage());
                            long backoff = Math.min((1L << (attempt - 1)) * 1000L, 30000L);
                            long jitter = ThreadLocalRandom.current().nextLong((long)(backoff*0.8), (long)(backoff*1.2));
                            TimeUnit.MILLISECONDS.sleep(jitter);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("Worker error", e);
            }
        }
    }
    private BsonDocument loadToken() {
        return tokenRepo.findById("mongoToEsSync")
                .map(ChangeStreamState::getResumeToken)
                .map(d -> BsonDocument.parse(d.toJson()))
                .orElse(null);
    }

    private void saveToken(BsonDocument token) {
        Document tokenDoc = Document.parse(token.toJson());
        ChangeStreamState state = new ChangeStreamState();
        state.setId("mongoToEsSync");
        state.setResumeToken(tokenDoc);
        state.setLastUpdated(Instant.now());
        tokenRepo.save(state);
        this.resumeToken = token;
    }

    private BsonTimestamp getCurrentTimestamp() {
        Document hello = mongoClient.getDatabase("admin").runCommand(new Document("hello", 1));
        return hello.get("operationTime", BsonTimestamp.class);
    }

    private Entity toEntity(ChangeStreamDocument<Document> change) {
        Document d = change.getFullDocument();
        ObjectId oid = d.getObjectId("_id");
        return Entity.builder()
                .id(oid != null ? oid.toHexString() : null)
                .name(d.getString("name"))
                .createTime(LocalDateTime.ofInstant(d.getDate("createTime").toInstant(),
                        ZoneId.systemDefault()))
                .modifiedTime(LocalDateTime.ofInstant(d.getDate("modifiedTime").toInstant(),
                        ZoneId.systemDefault()))
                .version(d.getLong("version"))
                .isDeleted(d.getBoolean("isDeleted"))
                .build();
    }
}

