package com.packages.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import com.mongodb.client.model.Filters;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.packages.model.ConsistencyResult;
import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

@Service
public class ConsistencyService {

    private final MongoTemplate          mongo;
    private final ElasticsearchClient    es;
    private MessageDigest                sha256;

    public ConsistencyService(MongoTemplate mongo, ElasticsearchClient es) {
        this.mongo = mongo;
        this.es    = es;
    }

    @PostConstruct
    void init() throws NoSuchAlgorithmException {
        this.sha256 = MessageDigest.getInstance("SHA-256");
    }

    public ConsistencyResult calculate() throws IOException {
        MongoCollection<Document> coll = mongo.getCollection("Entity");
        List<String> ids = new ArrayList<>();
        try (MongoCursor<String> cursor =
                     coll.distinct("_id", String.class).iterator()) {
            while (cursor.hasNext()) {
                ids.add(cursor.next());
            }
        }

        long total   = ids.size();
        long matches = 0;

        for (String id : ids) {
            Document mdoc = coll.find(Filters.eq("_id", id)).first();
            String  mjson = mdoc == null ? "" : mdoc.toJson();
            byte[] mhash = sha256.digest(
                    mjson.getBytes(StandardCharsets.UTF_8)
            );
            GetResponse<Document> eres = es.get(g -> g
                            .index("entity")
                            .id(id),
                    Document.class);

            String ejson = "";
            if (eres.found() && eres.source() != null) {
                ejson = eres.source().toJson();
            }
            byte[] ehash = sha256.digest(
                    ejson.getBytes(StandardCharsets.UTF_8)
            );
            if (java.util.Arrays.equals(mhash, ehash)) {
                matches++;
            }
            sha256.reset();
        }

        double pct = total == 0 ? 100.0
                : (matches * 100.0) / total;
        return new ConsistencyResult(total, matches, pct);
    }
}
