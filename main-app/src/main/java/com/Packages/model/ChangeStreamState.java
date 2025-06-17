//package com.Packages.model;
//
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import org.springframework.data.annotation.Id;
//import org.springframework.data.mongodb.core.mapping.Document;
//import org.bson.BsonDocument;
//import java.time.Instant;
//
//
//@Data
//@Builder
//@NoArgsConstructor
//@AllArgsConstructor
//@Document("changeStreamState")
//public class ChangeStreamState {
//    @Id
//    private String id;
//
//    private BsonDocument resumeToken;
//    private Instant    lastUpdated;
//}
