package com.Packages.controller;

import com.Packages.dto.EntityDTO;
import com.Packages.service.HybridSyncService;
import com.Packages.service.KafkaSyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/api/entity/hybrid")
public class HybridSyncController {
    @Autowired
    private HybridSyncService hybridSyncService;

    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO) {
        return hybridSyncService.createEntity(entityDTO);
    }

    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@PathVariable String documentId, @RequestBody EntityDTO entityDTO) {
        return hybridSyncService.updateEntity(documentId, entityDTO);
    }

    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@PathVariable String documentId) {
        return hybridSyncService.deleteEntity(documentId);
    }
}
