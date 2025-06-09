package com.Packages.Model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityMetadata {
    private String metaId;
    private String entityId ;
    private String operation;
    private Long operationSeq;
    private Long mongoWriteMillis;
    private Long esSyncMillis;
    private Integer syncAttempt;
    private String mongoStatus;
    private String esStatus;
    private String dlqReason;
}
