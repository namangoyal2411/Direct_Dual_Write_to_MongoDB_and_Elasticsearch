package com.Packages.Model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityMetadata {
    @Id
    private String metaId;
    private String entityId ;
    private String operation;
    private Long operationSeq;
    private Long mongoWriteMillis;
    private Long esSyncMillis;
    private Long firstFailureTime;
    private Integer syncAttempt;
    private String mongoStatus;
    private String esStatus;
    private String dlqReason;
}
