package com.packages.model;

public record ConsistencyResult(
        long total,
        long matches,
        double percent
) {
}