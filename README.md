# Project Setup Guide – Spring Boot + Kafka + Elasticsearch + Kibana
This guide explains how I’ve set up my development environment locally (without Docker) to run a Spring Boot application integrated with Kafka, Elasticsearch, and Kibana.
## Tools Used
- Spring Boot (generated via https://start.spring.io)
- Apache Kafka (downloaded and run locally)
- Elasticsearch (installed locally)
- Kibana (installed locally)
- Java 17+
- Maven
## Project Overview
This Spring Boot app performs the following:
- Produces and consumes messages using Kafka
- Stores and searches entity data using Elasticsearch
- Uses Kibana to monitor and visualize data from Elasticsearch
## Setup Steps
### 1. Spring Boot Application
Generated from https://start.spring.io with the following dependencies:
- Spring Web
- Spring Kafka
- Lombok
- Elasticsearch Java Client
- Spring Boot DevTools
- Spring Configuration Processor
Run the application:
Make sure Kafka, Elasticsearch, and Kibana are running before you start the Spring Boot app.
### 2. Kafka Setup
Downloaded from: [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)
### Steps to Start Kafka
# Step 1: Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
### 3. Elasticsearch Setup
Downloaded from: [https://www.elastic.co/downloads/elasticsearch](https://www.elastic.co/downloads/elasticsearch)
Start Elasticsearch:
cd elasticsearch-x.y.z/
bin/elasticsearch
Once started, Elasticsearch will be accessible at:
http://localhost:9200
### 4. Kibana Setup
Downloaded from: https://www.elastic.co/downloads/kibana
Start Kibana:
cd kibana-x.y.z/
bin/kibana
Kibana UI will be accessible at:
http://localhost:5601
Kafka
spring.kafka.bootstrap-servers=localhost:9092
Elasticsearch
elasticsearch.host=localhost
elasticsearch.port=9200
