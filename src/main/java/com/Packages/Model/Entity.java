package com.Packages.Model;


import com.Packages.DTO.EntityDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Entity")
public class Entity {
    @Id
    String id;
    String name;
    LocalDateTime createTime;
    LocalDateTime modifiedTime;

    public static Entity fromDTO(EntityDTO dto) {
        return Entity.builder()
                .id(dto.getId())
                .name(dto.getName())
                .createTime(dto.getCreateTime())
                .modifiedTime(dto.getModifiedTime())
                .build();
    }
}
