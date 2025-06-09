package com.Packages.DTO;

import com.Packages.Model.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityDTO {
    @Id
    String id ;
    String name ;
    private LocalDateTime createTime;
    private LocalDateTime modifiedTime;
    public static EntityDTO fromEntity(Entity entity) {
        if (entity == null) return null;
        return EntityDTO.builder()
                .id(entity.getId())
                .name(entity.getName())
                .createTime(entity.getCreateTime())
                .modifiedTime(entity.getModifiedTime())
                .build();
    }
}
