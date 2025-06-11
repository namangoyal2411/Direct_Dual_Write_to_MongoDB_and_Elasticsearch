package com.Packages.Test;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.Packages.Repository.ControlledElasticRepository;
import com.Packages.Repository.EntityElasticRepository;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("test")
public class TestElasticConfig {
//
//  @Bean
//  public EntityElasticRepository controlledElasticRepository(
//          ElasticsearchClient elasticsearchClient,
//          @Value("${es.test.successRate:0.1}") double successRate) {
//    System.out.println("ControlledElasticRepository initialized with success rate: " + successRate);
//    return new ControlledElasticRepository(elasticsearchClient, successRate);
//  }
}
