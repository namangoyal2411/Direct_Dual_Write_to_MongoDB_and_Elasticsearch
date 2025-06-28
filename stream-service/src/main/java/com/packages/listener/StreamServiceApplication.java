package com.packages.listener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;


@SpringBootApplication
@ComponentScan(basePackages = "com.packages")
@EnableMongoRepositories(basePackages = "com.packages.repository")
@EntityScan(basePackages = "com.packages.model")
public class StreamServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamServiceApplication.class, args);
    }
}