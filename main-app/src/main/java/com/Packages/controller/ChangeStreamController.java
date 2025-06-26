package com.Packages.controller;

import com.Packages.dto.EntityDTO;
import com.Packages.service.ChangeStreamService;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/entity/stream")
public class ChangeStreamController {

    private final ChangeStreamService changeStreamService;

    public ChangeStreamController(ChangeStreamService changeStreamService) {
        this.changeStreamService = changeStreamService;
    }

    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO create(@RequestBody EntityDTO entityDTO) {
        return changeStreamService.createEntity(entityDTO);
    }

    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.CREATED)
    public EntityDTO updateEntity(@PathVariable String documentId, @RequestBody EntityDTO entityDTO) {
        return changeStreamService.updateEntity(documentId, entityDTO);
    }

    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean delete(@PathVariable String documentId) {
        return changeStreamService.deleteEntity(documentId);
    }


}
