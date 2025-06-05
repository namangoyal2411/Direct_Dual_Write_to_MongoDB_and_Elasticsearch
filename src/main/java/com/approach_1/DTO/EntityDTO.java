package com.approach_1.DTO;

import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;
@Data
@Builder
public class EntityDTO {
    @Id
    String id ;
    String name ;
}
