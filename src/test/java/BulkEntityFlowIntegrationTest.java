
import com.Packages.DTO.EntityDTO;
import com.Packages.ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication;
import com.Packages.Repository.EntityMetadataRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = ReliableAndResilientDataSyncBetweenMongoDBAndElasticsearchApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ActiveProfiles("test")

public class BulkEntityFlowIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private EntityMetadataRepository metadataRepo;

    @Test
    void bulkFlow_writesAllMetadata() {
        int entityCount = 200;
        int maxUpdates   = 5;
        Random rand      = new Random();
        Map<String,Integer> expectedSeq = new HashMap<>();
        for (int i = 0; i < entityCount; i++) {
            String id = UUID.randomUUID().toString();
            LocalDateTime now = LocalDateTime.now();
            EntityDTO dto = new EntityDTO(id, "Name-"+i, now, now);
            restTemplate.postForEntity("/api/entity/kafka/create", dto, EntityDTO.class);
            int ops = 1;
            int updates = rand.nextInt(maxUpdates+1);
            for (int u = 1; u <= updates; u++) {
                dto.setName("Name-"+i+"-v"+u);
                restTemplate.put("/api/entity/kafka/update/{id}", dto, id);
                ops++;
            }
            if (rand.nextBoolean()) {
                restTemplate.delete("/api/entity/kafka/delete/{id}", id);
                ops++;
            }
            expectedSeq.put(id, ops);
        }
        expectedSeq.forEach((id, seq) -> {
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> {
                        long latest = metadataRepo.getLatestOperationSeq(id);

                        assertThat(latest)
                                .as("operationSeq for entity %s", id)
                                .isGreaterThanOrEqualTo(seq);
                    });
        });
    }
}
