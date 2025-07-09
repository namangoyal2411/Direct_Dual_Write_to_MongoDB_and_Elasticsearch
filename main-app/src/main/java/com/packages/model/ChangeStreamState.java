package com.packages.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document("changeStreamState")
public class ChangeStreamState {
    @Id
    private String id;
    private org.bson.Document resumeToken;
    private Instant lastUpdated;
}
