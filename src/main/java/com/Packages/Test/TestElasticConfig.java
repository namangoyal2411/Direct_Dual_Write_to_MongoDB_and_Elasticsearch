package com.Packages.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.Packages.Repository.ControlledElasticRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("test")
public class TestElasticConfig {

  @Bean
  public ControlledElasticRepository controlledElasticRepository(
          ElasticsearchClient elasticsearchClient,
          @Value("${es.test.successRate:0.8}") double successRate) {

    return new ControlledElasticRepository(elasticsearchClient, successRate);
  }
}
