# Real-Time Environmental Data Streaming Pipeline

## Overview
This project implements a real-time environmental data streaming pipeline using modern data engineering tools. The system simulates water quality sensor readings and processes them through a distributed pipeline for storage and analysis.

The architecture uses Docker containers to orchestrate multiple services including Kafka, Spark Structured Streaming, and PostgreSQL.

The pipeline demonstrates how streaming data can be ingested, processed, and analyzed in real time.

---

## Architecture

Python Producer → Kafka → Spark Structured Streaming → PostgreSQL

### Components

Producer (Python)  
Simulates environmental sensor data such as pH and temperature and sends messages to Kafka.

Kafka  
Acts as the message broker that streams sensor data to downstream consumers.

Spark Structured Streaming  
Consumes Kafka messages, processes the data in micro-batches, and writes the results to PostgreSQL.

PostgreSQL  
Stores processed sensor readings for analysis.

Docker Compose  
Orchestrates all services and ensures reproducible environments.

---

## Technologies Used

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker
- Docker Compose

---

## Project Structure

real-time-env-streaming-pipeline
│
├── producer
│   └── producer.py
│
├── spark
│   └── streaming_job.py
│
├── sql
│   └── init.sql
│
├── docker-compose.yml
│
├── screenshots
│
└── README.md

---

### 2. Start the services

docker compose up -d

This command starts the following services:

- Kafka
- Zookeeper
- Spark
- PostgreSQL
- Producer

---

### 3. Verify containers are running

docker compose ps

Example output:

kafka        Up (healthy)
postgres     Up
producer     Up
spark        Up
zookeeper    Up

---

## Querying the Data

Once the pipeline is running, you can query the stored sensor readings.

### Connect to PostgreSQL

docker exec -it real-time-env-streaming-pipeline-postgres-1 psql -U postgres -d waterdb

---

## Example Queries

### Total number of sensor readings

SELECT COUNT(*) FROM sensor_readings;

---

### View latest readings

SELECT *
FROM sensor_readings
ORDER BY event_time DESC
LIMIT 10;

---

### Average pH value

SELECT AVG(ph) FROM sensor_readings;

---

### Average pH by location

SELECT location, AVG(ph)
FROM sensor_readings
GROUP BY location;

---

## Example Analytics

The pipeline enables real-time environmental monitoring such as:

- Monitoring water pH levels
- Detecting abnormal pH values
- Tracking temperature changes
- Comparing sensor readings across locations

These analytics demonstrate how streaming data pipelines can support environmental monitoring and decision-making.

---

## Screenshots

Example screenshots to include in the repository:

- Docker containers running
- Spark streaming logs
- PostgreSQL query results
- Sensor readings stored in the database

Example folder structure:

screenshots/
    containers-running.png
    spark-streaming-logs.png
    postgres-query-results.png

---

## Key Concepts Demonstrated

This project demonstrates several important data engineering concepts:

- Real-time data streaming
- Message queue architecture
- Micro-batch stream processing
- Containerized distributed systems
- Data pipeline orchestration

- SQL-based analytics
