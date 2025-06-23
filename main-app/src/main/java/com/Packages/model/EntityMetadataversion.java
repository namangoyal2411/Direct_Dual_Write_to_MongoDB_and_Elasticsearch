package com.Packages.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EntityMetadataversion {
    @Id
    private String metaId;
    private String entityId;
    private String approach;
    private String operation;
    private Long mongoWriteMillis;
    private Long esSyncMillis;
    private Long firstFailureTime;
    private Integer syncAttempt;
    private String mongoStatus;
    private String esStatus;
    private String dlqReason;
    @Version
    private long version;
}
