//package com.packages.util;
//
//import com.packages.model.Entity;
//import com.packages.model.EntityMetadata;
//
//import java.time.LocalDateTime;
//
//public class EntityMetadataUtil {
//    private EntityMetadataUtil() {
//
//    }
//    public EntityMetadata buildMetadata(String metaId,String entityId,
//                                        String operation,
//                                        long operationSeq,
//                                        Long mongoWriteMillis) {
//        return EntityMetadata.builder()
//                .metaId(metaId)
//                .entityId(entityId)
//                .approach("Direct Data Transfer")
//                .operation(operation)
//                .operationSeq(operationSeq)
//                .mongoWriteMillis(mongoWriteMillis)
//                .syncAttempt(1)
//                .esStatus("pending")
//                .build();
//    }
//    public EntityMetadata buildMetadata(String metaId,String entityId,
//                                        String operation,
//                                        long operationSeq) {
//        return buildMetadata(metaId, entityId, operation, operationSeq, null);
//    }
//}
