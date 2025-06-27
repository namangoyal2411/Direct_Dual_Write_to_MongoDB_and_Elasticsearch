package com.packages.controller;

import com.packages.exception.EntityNotFoundException;
import com.packages.model.Entity;
import com.packages.service.EntityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/entity/hybrid")
public class EntityController {
    @Autowired
    private EntityService entityService;

    @PostMapping("/create")
    @ResponseStatus(HttpStatus.CREATED)
    public Entity createEntity(@RequestBody Entity entity) {
        return entityService.createEntity(entity);
    }

    @PutMapping("/update/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public Entity updateEntity(@PathVariable String documentId, @RequestBody Entity entity) {
        return entityService.updateEntity(documentId, entity);
    }

    @DeleteMapping("/delete/{documentId}")
    @ResponseStatus(HttpStatus.OK)
    public boolean deleteEntity(@PathVariable String documentId) {
        return entityService.deleteEntity(documentId);
    }

    @ExceptionHandler(EntityNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleNotFound(EntityNotFoundException ex) {
        return new ErrorResponse("NOT_FOUND", ex.getMessage());
    }
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleGeneric(Exception ex) {
        return new ErrorResponse("INTERNAL_ERROR", ex.getMessage());
    }

    public static class ErrorResponse {
        private final String error;
        private final String message;

        public ErrorResponse(String error, String message) {
            this.error   = error;
            this.message = message;
        }

        public String getError()   { return error; }
        public String getMessage() { return message; }
    }
}