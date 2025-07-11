
# Project Setup Guide – Spring Boot + Kafka + Elasticsearch + Kibana + MongoDB

This guide explains how I’ve set up my development environment locally (without Docker) to run a Spring Boot application integrated with Kafka, Elasticsearch, Kibana, and MongoDB.

---

## Tools Used

- Spring Boot (generated via [https://start.spring.io](https://start.spring.io))
- Apache Kafka (downloaded and run locally)
- Elasticsearch (installed locally)
- Kibana (installed locally)
- MongoDB (installed locally)
- Java 17+
- Maven

---

## Project Overview

This Spring Boot app performs the following:

- Produces and consumes messages using **Kafka**
- Stores and searches entity data using **Elasticsearch**
- Stores raw or processed data in **MongoDB**
- Uses **Kibana** to monitor and visualize data from Elasticsearch

---

## Setup Steps

### 1. Spring Boot Application

Generated using [https://start.spring.io](https://start.spring.io) with the following dependencies:

- Spring Web  
- Spring Kafka  
- Spring Data MongoDB  
- Lombok  
- Elasticsearch Java Client  
- Spring Boot DevTools  
- Spring Configuration Processor

Make sure Kafka, MongoDB, Elasticsearch, and Kibana are running before starting the application.

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

You can verify it's running with:

```bash
curl http://localhost:9200
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

### 5. MongoDB Setup

Download MongoDB from: [https://www.mongodb.com/try/download/community](https://www.mongodb.com/try/download/community)

#### Start MongoDB

If you installed MongoDB via your system's package manager, you can typically start it with:

```bash
mongod
```

MongoDB will be accessible at:

```
mongodb://localhost:27017
```

You can connect using the Mongo shell:

```bash
mongo
```

Or using any MongoDB client like MongoDB Compass.

---

## Spring Boot Configuration (`application.properties`)

```properties
# Kafka
spring.kafka.bootstrap-servers=localhost:9092

# Elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200

# MongoDB
spring.data.mongodb.host=localhost
spring.data.mongodb.port=27017
spring.data.mongodb.database=my-database
```
## Commands for opening Javadoc 
 ```open main-app/build/docs/javadoc/index.html  
    open stream-service/build/docs/javadoc/index.html
    open consumer-service/build/docs/javadoc/index.html
```

