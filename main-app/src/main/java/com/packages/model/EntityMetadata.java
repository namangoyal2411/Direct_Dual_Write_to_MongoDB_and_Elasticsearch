package com.packages.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "EntityMetadata")
public class EntityMetadata {
    @Id
    private String metaId;
    private String entityId;
    private String approach;
    private String operation;
    private Long operationSeq;
    private Long mongoWriteMillis;
    private Long esSyncMillis;
    private Long firstFailureTime;
    private String esStatus;
    private String failureReason;
}