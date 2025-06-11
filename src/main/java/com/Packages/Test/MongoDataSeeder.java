//package com.Packages.Test;
//
//import com.Packages.DTO.EntityDTO;
//import com.Packages.Model.Entity;
//import com.Packages.Repository.EntityMongoRepository;
//import com.Packages.Service.MongoKafkaElasticService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.stereotype.Component;
//
//import java.time.LocalDateTime;
//import java.util.UUID;
//
//@Component
//public class MongoDataSeeder implements CommandLineRunner {
//    @Autowired
//    private MongoKafkaElasticService mongoKafkaElasticService;
//    @Override
//    public void run(String... args) throws Exception {
//        for (int i = 0; i < 100; i++) {
//            EntityDTO entityDTO = EntityDTO.builder().
//                                id(UUID.randomUUID().toString()).
//                                name("Test Entity " + i).
//                                createTime(LocalDateTime.now()).
//                                modifiedTime(LocalDateTime.now()).
//                                build();
//            mongoKafkaElasticService.createEntity(entityDTO,"entity");
//        }
//    }
//}
