package com.Packages.Controller;

import com.Packages.DTO.EntityDTO;
import com.Packages.Service.MongoKafkaElasticService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/entity/kafka")
public class MongoKafkaElasticController {
    @Autowired
    private MongoKafkaElasticService mongoKafkaElasticService;
    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO, @RequestParam String indexName){
        return mongoKafkaElasticService.createEntity(entityDTO, indexName);
    }
    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@RequestParam String indexName,@PathVariable String documentId, @RequestBody EntityDTO entityDTO ){
        return mongoKafkaElasticService.updateEntity(indexName,documentId,entityDTO);
    }
    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@RequestParam String indexName,@PathVariable String documentId){
        return mongoKafkaElasticService.deleteEntity(indexName,documentId);
    }
    @PostMapping("/test/load")
    public ResponseEntity<String> loadTestData(@RequestParam(defaultValue = "100") int count) {
        for (int i = 0; i < count; i++) {
            String id = "test-id-" + i;
            EntityDTO dto = new EntityDTO();
            dto.setId(id);
            dto.setName("Test Entity " + i);
            dto.setCreateTime(LocalDateTime.now());
            dto.setModifiedTime(LocalDateTime.now());
            try {
                mongoKafkaElasticService.createEntity(dto, "entity");
            } catch (Exception e) {
                System.err.println("Error creating entity #" + i + ": " + e.getMessage());
            }
            try {
                dto.setName("Updated Test Entity " + i);
                dto.setModifiedTime(LocalDateTime.now());
                mongoKafkaElasticService.updateEntity("entity",dto.getId(),dto);
            } catch (Exception e) {
                System.err.println("Error updating entity #" + i + ": " + e.getMessage());
            }
//            if (i % 10 == 0) {
//                try {
//                    mongoKafkaElasticService.deleteEntity("entity", id);
//                } catch (Exception e) {
//                    System.err.println("Error deleting entity #" + i + ": " + e.getMessage());
//                }
//            }
        }
        return ResponseEntity.ok("Test data load complete for " + count + " entities");
    }

}
