package com.packages.controller;
import com.packages.model.ConsistencyResult;
import com.packages.service.ConsistencyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/api/hybrid/consistency")
public class ConsistencyController {

    private final ConsistencyService service;

    public ConsistencyController(ConsistencyService service) {
        this.service = service;
    }

    @GetMapping
    public ConsistencyResult getConsistency() throws IOException {
        return service.calculate();
    }
}