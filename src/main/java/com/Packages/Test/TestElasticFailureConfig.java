package com.Packages.Test;

import com.Packages.Repository.EntityElasticRepository;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class TestElasticFailureConfig {
    @Bean
    public EntityElasticRepository entityElasticRepository() {
        EntityElasticRepository mockRepo = Mockito.mock(EntityElasticRepository.class);
        Mockito.doThrow(new RuntimeException("Simulated ES down"))
                .when(mockRepo).createEntity(Mockito.any(), Mockito.any());
        return mockRepo;
    }
}
