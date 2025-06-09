package com.Packages.Model;

import com.Packages.DTO.EntityDTO;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntityEvent {
    private EntityDTO entityDTO;
    private String operation;
    private String id;
    private String index;
}
