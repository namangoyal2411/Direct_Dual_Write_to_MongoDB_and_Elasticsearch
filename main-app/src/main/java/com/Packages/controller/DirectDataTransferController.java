package com.Packages.controller;

import com.Packages.dto.EntityDTO;
import com.Packages.service.DirectDataTransferService;
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
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO) {
        return directDataTransferService.createEntity(entityDTO);
    }

    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@PathVariable String documentId, @RequestBody EntityDTO entityDTO) {
        return directDataTransferService.updateEntity(documentId, entityDTO);
    }

    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@PathVariable String documentId) {
        return directDataTransferService.deleteEntity(documentId);
    }
}
