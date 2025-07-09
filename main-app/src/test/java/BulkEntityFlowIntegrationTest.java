//
//import com.Packages.dto.EntityDTO;
//import com.Packages.ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication;
//import com.Packages.repository.EntityMetadataRepository;
//import com.Packages.service.ChangeStreamListenerService;
//import org.awaitility.Awaitility;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.web.client.TestRestTemplate;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.test.context.ActiveProfiles;
//
//import java.time.LocalDateTime;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Random;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//@SpringBootTest(
//        classes = {
//                ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication.class
//                ,com.packages.listener.StreamServiceApplication.class
//        },
//        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
//)
//@ActiveProfiles({"test","stream"})
//
//public class BulkEntityFlowIntegrationTest {
//    @Autowired TestRestTemplate restTemplate;
//    @Autowired MongoTemplate     mongoTemplate;
//    @Autowired
//    ChangeStreamListenerService listener;
//    @Autowired EntityMetadataRepository metadataRepo;
//    @Test
//    void bulkFlow_writesAllMetadata() {
//        int entityCount = 50;
//        int maxUpdates   = 1;
//        Random rand      = new Random();
//        for (int i = 0; i < entityCount; i++) {
//            String id = UUID.randomUUID().toString();
//            LocalDateTime now = LocalDateTime.now();
//            EntityDTO dto = new EntityDTO(id, "Name-"+i, now, now);
//            //restTemplate.postForEntity("/api/entity/kafka/create", dto, EntityDTO.class);
//            String id1 = UUID.randomUUID().toString();
//            EntityDTO dto1 = new EntityDTO(id1, "Name-"+i, now, now);
//            //restTemplate.postForEntity("/api/entity/create", dto1, EntityDTO.class);
//            String id2 = UUID.randomUUID().toString();
//            EntityDTO dto2 = new EntityDTO(id2, "Name-"+i, now, now);
//            restTemplate.postForEntity("/api/entity/stream/create", dto2, EntityDTO.class);
//            int updates = rand.nextInt(maxUpdates+1);
//            for (int u = 1; u <= updates; u++) {
//                dto.setName("Name-"+i+"-v"+u);
//                //  restTemplate.put("/api/entity/kafka/update/{id}", dto, id);
//                // restTemplate.put("/api/entity/update/{id}", dto1, id1);
//                restTemplate.put("/api/entity/stream/update/{id}", dto2, id2);
//            }
//            if (rand.nextBoolean()) {
//                //   restTemplate.delete("/api/entity/kafka/delete/{id}", id);
//                // restTemplate.delete("/api/entity/delete/{id}", id1);
//                restTemplate.delete("/api/entity/stream/delete/{id}", id2);
//            }
//        }
//
//    }
//}