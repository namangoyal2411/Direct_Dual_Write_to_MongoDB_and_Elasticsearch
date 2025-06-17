//package com.Packages.controller;
//
//import com.Packages.service.ChangeStreamService;
//import org.bson.BsonDocument;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.web.bind.annotation.*;
//
//import java.time.Instant;
//import java.util.Map;
//
//@RestController
//@RequestMapping("/api/change-stream")
//public class ChangeStreamController {
//
//    @Autowired
//    private ChangeStreamService changeStreamService;
//
//    @PostMapping("/start")
//    @ResponseStatus(HttpStatus.OK)
//    public void startListener() {
//        changeStreamService.startListening();
//    }
//
//    @PostMapping("/stop")
//    @ResponseStatus(HttpStatus.OK)
//    public void stopListener() {
//        changeStreamService.stop();
//    }
//
//    @PostMapping("/restart")
//    @ResponseStatus(HttpStatus.OK)
//    public void restartListener() {
//        changeStreamService.stop();
//        changeStreamService.startListening();
//    }
//
//    @GetMapping("/status")
//    @ResponseStatus(HttpStatus.OK)
//    public Map<String, Object> getStatus() {
//        BsonDocument token = changeStreamService.getResumeToken();
//        Instant      when  = changeStreamService.getLastUpdated();
//        return Map.of(
//                "lastResumeToken", token,
//                "lastUpdated"    , when
//        );
//    }
//}
