package com.Packages.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityEvent {
    private Entity entity;
    private String operation;
    private String id;
    private String index;
    private EntityMetadata entityMetadata;
}
