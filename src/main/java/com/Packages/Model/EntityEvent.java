package com.Packages.Model;

import com.Packages.DTO.EntityDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EntityEvent {
    private EntityDTO entityDTO;
    private String operation;
    private String id;
    private String index;
    private EntityMetadata metadata;
}
