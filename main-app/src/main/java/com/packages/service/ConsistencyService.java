package com.packages.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.get.GetResult;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.mget.MultiGetResponseItem;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.client.MongoCursor;
import com.packages.model.ConsistencyResult;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;

/** Compares every Mongo Entity with its ES counterpart in batches. */
@Service
public class ConsistencyService {

    private final MongoTemplate       mongo;
    private final ElasticsearchClient es;

    private ObjectMapper  mapper;
    private MessageDigest sha256;

    /** Tune for memory / speed trade-off. */
    private static final int    BATCH_SIZE = 1_000;
    private static final String ES_INDEX   = "entity";

    public ConsistencyService(MongoTemplate mongo, ElasticsearchClient es) {
        this.mongo = mongo;
        this.es    = es;
    }

    @PostConstruct
    void init() throws NoSuchAlgorithmException {
        mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        sha256 = MessageDigest.getInstance("SHA-256");
    }

    /* ------------------------------------------------------------------ */
    /*  PUBLIC API                                                        */
    /* ------------------------------------------------------------------ */
    public ConsistencyResult calculate() throws IOException {

        long total   = 0;
        long matches = 0;

        try (MongoCursor<Document> cursor = mongo.getCollection("Entity")
                .find()
                .batchSize(BATCH_SIZE)
                .cursor()) {

            List<String>   ids   = new ArrayList<>(BATCH_SIZE);
            List<Document> batch = new ArrayList<>(BATCH_SIZE);

            while (cursor.hasNext()) {

                /* ----------- collect one Mongo batch ------------- */
                ids.clear();
                batch.clear();
                for (int i = 0; i < BATCH_SIZE && cursor.hasNext(); i++) {
                    Document mdoc = cursor.next();
                    batch.add(mdoc);
                    ids.add(toHexId(mdoc));          // safe for ObjectId/String
                }

                /* ------------- multi-get from ES ----------------- */
                MgetResponse<Document> esResp = es.mget(
                        req -> req.index(ES_INDEX).ids(ids),
                        Document.class);

                /* ------------- compare each pair ----------------- */
                List<MultiGetResponseItem<Document>> items = esResp.docs();

                for (int i = 0; i < items.size(); i++) {
                    Document  mdoc = batch.get(i);
                    GetResult<Document> gres = items.get(i).result();

                    Document esDoc = gres.found() ? gres.source() : null;

                    byte[] mHash = normalisedHash(mdoc, false);
                    byte[] eHash = normalisedHash(
                            esDoc != null ? esDoc : mdoc,
                            esDoc == null        /* mark “missing in ES” */ );

                    total++;
                    if (Arrays.equals(mHash, eHash)) {
                        matches++;
                    }
                }
            }
        }

        double pct = total == 0 ? 100.0 : (matches * 100.0) / total;
        return new ConsistencyResult(total, matches, pct);
    }
    private static String toHexId(Document doc) {
        Object raw = doc.get("_id");
        return (raw instanceof ObjectId oid) ? oid.toHexString()
                : String.valueOf(raw);
    }

    private byte[] normalisedHash(Document src, boolean missing) throws IOException {

        Map<String,Object> norm = new LinkedHashMap<>();

        norm.put("id", extractId(src));                // <-- use new helper
        norm.put("name", src.getString("name"));

        norm.put("createTime",   epochMillis(src.get("createTime")));
        norm.put("modifiedTime", epochMillis(src.get("modifiedTime")));

        Object delRaw = src.get("isDeleted");
        if (delRaw == null) delRaw = src.get("deleted");
        boolean isDel = (delRaw instanceof Boolean b) ? b : false;
        norm.put("isDeleted", isDel);

        Object vObj = src.get("version");
        long ver    = (vObj instanceof Number n) ? n.longValue() : 0L;
        norm.put("version", ver);

        if (missing) norm.put("_missing", true);

        byte[] json = mapper.writeValueAsBytes(norm);
        byte[] hash = sha256.digest(json);
        sha256.reset();
        return hash;
    }
    private static String extractId(Document doc) {
        Object raw = doc.get("_id");
        if (raw == null) raw = doc.get("id");          // <-- ES uses "id"
        if (raw instanceof ObjectId oid) return oid.toHexString();
        return raw == null ? "" : raw.toString();
    }
    private static long epochMillis(Object raw) {
        if (raw instanceof Number n)         return n.longValue();
        if (raw instanceof java.util.Date d) return d.getTime();
        if (raw instanceof String s)         return Instant.parse(s).toEpochMilli();

        /* ES date_nanos array -> epoch-ms (local JVM zone, i.e. IST) */
        if (raw instanceof List<?> arr && arr.size() >= 7) {
            int y  = ((Number) arr.get(0)).intValue();
            int mo = ((Number) arr.get(1)).intValue();
            int d  = ((Number) arr.get(2)).intValue();
            int h  = ((Number) arr.get(3)).intValue();
            int mi = ((Number) arr.get(4)).intValue();
            int se = ((Number) arr.get(5)).intValue();
            int ns = ((Number) arr.get(6)).intValue();
            return LocalDateTime.of(y, mo, d, h, mi, se, ns)
                    /* USE SYSTEM DEFAULT ZONE (IST) */
                    .atZone(ZoneId.systemDefault())
                    .toInstant()
                    .toEpochMilli();
        }
        return 0L;
    }
}
