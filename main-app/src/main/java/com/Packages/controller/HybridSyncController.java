package com.Packages.controller;

import com.Packages.dto.EntityDTO;
import com.Packages.service.HybridSyncService;
import com.Packages.service.HybridSyncServiceVersion;
import com.Packages.service.KafkaSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
@RestController
@RequestMapping("/api/entity/hybrid")
public class HybridSyncController {
    @Autowired
    private HybridSyncServiceVersion hybridSyncService;

    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO createEntity(@RequestBody EntityDTO entityDTO) {
        return hybridSyncService.createEntity(entityDTO);
    }
    private static final Logger log = LoggerFactory.getLogger(KafkaSyncController.class);


    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@PathVariable String documentId, @RequestBody EntityDTO entityDTO) {
        log.info("→ [Controller] PUT /update/{}  payload={}",documentId,entityDTO);
        return hybridSyncService.updateEntity(documentId, entityDTO);
    }

    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@PathVariable String documentId) {
        return hybridSyncService.deleteEntity(documentId);
    }
}
