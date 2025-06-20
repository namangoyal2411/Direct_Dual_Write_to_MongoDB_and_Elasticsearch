package com.Packages.listener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;


@SpringBootApplication
@ComponentScan(basePackages = "com.Packages")
@EnableMongoRepositories(basePackages = "com.Packages.repositoryinterface")
@EntityScan(basePackages = "com.Packages.model")
public class StreamServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamServiceApplication.class, args);
    }
}