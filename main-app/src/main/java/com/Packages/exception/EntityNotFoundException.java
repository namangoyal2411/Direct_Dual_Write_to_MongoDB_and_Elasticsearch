package com.Packages.exception;

public class EntityNotFoundException extends RuntimeException {
    public EntityNotFoundException(String id) {
        super("Entity not found for id: " + id);
    }
}
