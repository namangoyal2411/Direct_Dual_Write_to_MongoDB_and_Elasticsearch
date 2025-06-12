package com.Packages;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration;

@SpringBootApplication()
public class ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication.class, args);
    }
}
