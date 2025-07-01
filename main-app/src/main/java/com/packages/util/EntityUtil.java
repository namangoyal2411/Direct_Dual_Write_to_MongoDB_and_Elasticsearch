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
    public static Entity markDeleted(Entity oldEntity) {
        oldEntity.setDeleted(true);
        oldEntity.setModifiedTime(LocalDateTime.now());
        return oldEntity;
    }
}