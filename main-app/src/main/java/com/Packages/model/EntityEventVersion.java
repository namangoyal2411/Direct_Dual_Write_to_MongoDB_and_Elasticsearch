package com.Packages.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EntityEventVersion {
     private EntityMetadataversion entityMetadataversion;
     private String entityId;
     private Long version;
     private Entity entity;
}
