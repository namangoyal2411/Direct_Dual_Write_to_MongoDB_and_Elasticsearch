package com.packages.util;

import com.packages.model.Entity;
import com.packages.model.EntityMetadata;

import java.time.LocalDateTime;

public class EntityUtil {

    private EntityUtil() {

    }

    public static Entity updateEntity(Entity entity, Entity oldEntity) {
        oldEntity.setName(entity.getName());
        oldEntity.setModifiedTime(LocalDateTime.now());

        return oldEntity;
    }
}
