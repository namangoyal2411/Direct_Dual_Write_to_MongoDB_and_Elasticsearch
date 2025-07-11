package com.packages.util;

import com.packages.model.EntityMetadata;

public class EntityMetadataUtil {
    private EntityMetadataUtil() {
    }

    public static void applyUpdate(EntityMetadata meta,
                                   String status,
                                   Long esSyncMillis,
                                   String failureReason) {
        meta.setEsStatus(status);
        meta.setEsSyncMillis(esSyncMillis);
        meta.setFailureReason(failureReason);
    }
}
