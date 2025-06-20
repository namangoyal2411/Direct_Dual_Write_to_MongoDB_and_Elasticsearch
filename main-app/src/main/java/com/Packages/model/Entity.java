package com.Packages.model;


import com.Packages.dto.EntityDTO;
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
    private String id;
    private String name;
    private LocalDateTime createTime;
    private LocalDateTime modifiedTime;
    private String metadataId;
}
