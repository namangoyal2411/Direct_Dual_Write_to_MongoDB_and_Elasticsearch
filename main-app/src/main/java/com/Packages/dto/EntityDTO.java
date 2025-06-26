package com.Packages.dto;

import com.Packages.model.Entity;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EntityDTO {
    @Id
    String id;
    String name;
    private LocalDateTime createTime;
    private LocalDateTime modifiedTime;
}
