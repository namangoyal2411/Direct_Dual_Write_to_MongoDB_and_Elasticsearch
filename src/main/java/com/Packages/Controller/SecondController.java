package com.Packages.Controller;

import com.Packages.DTO.EntityDTO;
import com.Packages.Service.SecondApproach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/api/entity/kafka")
public class SecondController {
    @Autowired
    private SecondApproach secondApproach;
    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO, @RequestParam String indexName){
        return secondApproach.createEntity(entityDTO, indexName);
    }
    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@RequestParam String indexName,@PathVariable String documentId, @RequestBody EntityDTO entityDTO ){
        return secondApproach.updateEntity(indexName,documentId,entityDTO);
    }
    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@RequestParam String indexName,@PathVariable String documentId){
        return secondApproach.deleteEntity(indexName,documentId);
    }
}
