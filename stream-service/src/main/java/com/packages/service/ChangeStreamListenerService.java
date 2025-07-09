
package com.packages.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.packages.model.ChangeStreamState;
import com.packages.model.Entity;
import com.packages.model.EntityMetadata;
import com.packages.repository.ChangeStreamStateRepository;
import com.packages.repository.EntityElasticRepository;
import com.packages.repository.EntityMetadataRepository;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Service
@Profile("stream")
public class ChangeStreamListenerService {
    private static final Logger log = LoggerFactory.getLogger(ChangeStreamListenerService.class);
    private static final int MAX_RETRIES = 5;
    private static int counter = 0;
    private static final String DB_NAME = "Datasync";
    private static final String COLL_NAME = "Entity";

    private final ExecutorService listener = Executors.newSingleThreadExecutor();
    private final ExecutorService workers = Executors.newFixedThreadPool(10);
    private final ScheduledExecutorService retryer = Executors.newScheduledThreadPool(4);

    private final MongoClient mongoClient;
    private final EntityElasticRepository esRepo;
    private final EntityMetadataRepository metadataRepo;
    private final ChangeStreamStateRepository tokenRepo;
    private volatile BsonDocument resumeToken;
    private final EntityMetadataService entityMetadataService;

    @Autowired
    public ChangeStreamListenerService(
            MongoClient mongoClient,
            EntityElasticRepository esRepo,
            EntityMetadataRepository metadataRepo,
            ChangeStreamStateRepository tokenRepo,
            EntityMetadataService entityMetadataService) {
        this.mongoClient = mongoClient;
        this.esRepo = esRepo;
        this.metadataRepo = metadataRepo;
        this.tokenRepo = tokenRepo;
        this.resumeToken = loadToken();
        this.entityMetadataService = entityMetadataService;
    }

    @PostConstruct
    public void start() {
        listener.submit(this::listen);
    }

    @PreDestroy
    public void stop() {
        listener.shutdownNow();
        workers.shutdown();
        retryer.shutdown();
    }

    private void listen() {
        var database = mongoClient.getDatabase(DB_NAME);
        var coll = database.getCollection(COLL_NAME);

        ChangeStreamIterable<Document> stream = (resumeToken != null)
                ? coll.watch()
                .resumeAfter(resumeToken)
                .fullDocument(FullDocument.UPDATE_LOOKUP)
                : coll.watch()
                .startAtOperationTime(getCurrentTimestamp())
                .fullDocument(FullDocument.UPDATE_LOOKUP);

        List<ChangeStreamDocument<Document>> buffer = new ArrayList<>();
        stream.forEach(change -> {
           // log.info("Received entity: {}", counter++);
            workers.submit(() -> processChange(change, 0));
        });
    }
    public void processChange(ChangeStreamDocument<Document> change, int attempt) {
       // log.info("Checking attempt {}", attempt);
        String op = change.getOperationType().getValue();
        Entity entity = toEntity(change);
        long mongoTs = change.getClusterTime().getTime() * 1000L;

        try {
            switch (op) {
                case "insert" -> esRepo.createEntity("entity", entity);
                case "update", "replace" -> esRepo.updateEntity("entity", entity.getId(), entity);
                default -> {
                }
            }
            long esTs = System.currentTimeMillis();
            if (attempt==0) {
                entityMetadataService.createEntityMetadata(
                        entity,
                        op,
                        "success",
                        esTs,
                        mongoTs,
                        null
                );
            }
            else {
                String metaId = entity.getId() + "-" + op + "-" + entity.getVersion();
                entityMetadataService.updateEntityMetadata(metaId,"success",System.currentTimeMillis(),null);}
            saveToken(change.getResumeToken());
        } catch (Exception ex) {
            boolean isInvalidData = false;
            String reason = ex.getMessage();

            if (ex instanceof ElasticsearchException ee) {
                if (ee.error() != null) {
                    reason = ee.error().reason();
                }
                isInvalidData = (ee.status() == 400);
            }
            if (attempt==0) {
                entityMetadataService.createEntityMetadata(
                        entity,
                        op,
                        "failure",
                        null,
                        mongoTs,
                        reason
                );
            }
            else if (attempt>=MAX_RETRIES) {
                String metaId = entity.getId() + "-" + op + "-" + entity.getVersion();
                entityMetadataService.updateEntityMetadata(metaId,"failure",null,reason);
                saveToken(change.getResumeToken());
            }
            if (!isInvalidData && attempt < MAX_RETRIES) {
                long backoff = Math.min((1L << attempt) * 1000, 30000);
                long jitter = ThreadLocalRandom.current().nextLong((long) (backoff * 0.8), (long) (backoff * 1.2));
                log.warn("Scheduling retry #{} for entity {} in {} ms", attempt + 1, entity.getId(), jitter);
                retryer.schedule(() -> processChange(change, attempt + 1), jitter, TimeUnit.MILLISECONDS);
            }
        }
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

    private BsonTimestamp getCurrentTimestamp() {
        Document hello = mongoClient.getDatabase("admin")
                .runCommand(new Document("hello", 1));
        return hello.get("operationTime", BsonTimestamp.class);
    }

    private Entity toEntity(ChangeStreamDocument<Document> change) {
        Document doc = change.getFullDocument() != null
                ? change.getFullDocument()
                : change.getFullDocumentBeforeChange();
        ObjectId oid = doc.getObjectId("_id");
        String id = (oid != null ? oid.toHexString() : null);
        return Entity.builder()
                .id(id)
                .name(doc.getString("name"))
                .createTime(toLDT(doc.getDate("createTime")))
                .modifiedTime(toLDT(doc.getDate("modifiedTime")))
                .version(doc.getLong("version"))
                .isDeleted(doc.getBoolean("isDeleted"))
                .build();
    }

    private static LocalDateTime toLDT(java.util.Date d) {
        return d == null
                ? null
                : d.toInstant()
                .atZone(java.time.ZoneId.systemDefault())
                .toLocalDateTime();
    }
}


