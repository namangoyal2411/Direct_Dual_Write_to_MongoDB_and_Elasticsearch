package com.Packages.Controller;

import com.Packages.DTO.EntityDTO;
import com.Packages.Service.DirectDataTransferService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/entity")
public class DirectDataTransferController {
    @Autowired
    private DirectDataTransferService directDataTransferService;
    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO, @RequestParam String indexName){
       return directDataTransferService.createEntity(entityDTO, indexName);
    }
    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@RequestParam String indexName,@PathVariable String documentId, @RequestBody EntityDTO entityDTO ){
        return directDataTransferService.updateEntity(indexName,documentId,entityDTO);
    }
    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@RequestParam String indexName,@PathVariable String documentId){
        return directDataTransferService.deleteEntity(indexName,documentId);
    }


}
