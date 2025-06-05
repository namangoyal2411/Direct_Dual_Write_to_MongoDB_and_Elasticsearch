package com.approach_1.Model;


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
    String id ;
    String name ;
    LocalDateTime createTime ;
    LocalDateTime modifiedTime ;
}
