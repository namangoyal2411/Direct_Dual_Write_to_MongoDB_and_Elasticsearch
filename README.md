
# Project Setup Guide – Spring Boot + Kafka + Elasticsearch + Kibana

This guide explains how I’ve set up my development environment locally (without Docker) to run a Spring Boot application integrated with Kafka, Elasticsearch, and Kibana.

---

## Tools Used

- Spring Boot (generated via [https://start.spring.io](https://start.spring.io))
- Apache Kafka (downloaded and run locally)
- Elasticsearch (installed locally)
- Kibana (installed locally)
- Java 17+
- Maven

---

## Project Overview

This Spring Boot app performs the following:

- Produces and consumes messages using **Kafka**
- Stores and searches entity data using **Elasticsearch**
- Uses **Kibana** to monitor and visualize data from Elasticsearch

---

## Setup Steps

### 1. Spring Boot Application

Generated using [https://start.spring.io](https://start.spring.io) with the following dependencies:

- Spring Web  
- Spring Kafka  
- Lombok  
- Elasticsearch Java Client  
- Spring Boot DevTools  
- Spring Configuration Processor

Make sure Kafka, Elasticsearch, and Kibana are running before starting the application.

To run the application:

```bash
./mvnw spring-boot:run
````

---

### 2. Kafka Setup

Download Kafka from: [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

#### Start Kafka

```bash
# Step 1: Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
```

```bash
# Step 2: Start Kafka Broker (in a new terminal)
bin/kafka-server-start.sh config/server.properties
```

#### Create a Kafka Topic (Optional)

```bash
bin/kafka-topics.sh --create \
  --topic my-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

### 3. Elasticsearch Setup

Download from: [https://www.elastic.co/downloads/elasticsearch](https://www.elastic.co/downloads/elasticsearch)

#### Start Elasticsearch

```bash
cd elasticsearch-x.y.z/
bin/elasticsearch
```

Elasticsearch will be accessible at:

```
http://localhost:9200
```

---

### 4. Kibana Setup

Download from: [https://www.elastic.co/downloads/kibana](https://www.elastic.co/downloads/kibana)

#### Start Kibana

```bash
cd kibana-x.y.z/
bin/kibana
```

Kibana UI will be accessible at:

```
http://localhost:5601
```

---

## Spring Boot Configuration (`application.properties`)

```properties
# Kafka Configuration
spring.kafka.bootstrap-servers=localhost:9092

# Elasticsearch Configuration
elasticsearch.host=localhost
elasticsearch.port=9200
```
