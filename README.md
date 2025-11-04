# lennart-stage-2
# Stage 2 of the group project for ULPGC's Big Data course

This repository contains the Stage 2 implementation of the Search Engine project for the Big Data course.
We refactor the pipeline into independent services (Ingestion, Indexing, Search) using Java 17, Javalin, MongoDB, and Docker Compose.

---

## Overview

The system consists of several independent microservices that communicate through REST APIs and exchange JSON data:

| Component | Description |
|------------|--------------|
| **Ingestion API** | Handles the download and storage of book data. |
| **Indexing API** | Processes and structures the data, builds inverted indexes in MongoDB. |
| **Search API** | Provides keyword and metadata search endpoints. |
| **MongoDB** | Central database used by all services (Docker container or Atlas cloud). |
| **Control Module** | Simple orchestrator that triggers ingestion and indexing in sequence. |

## Web Interface
A simple web-based user interface was also developed to demonstrate the search functionality and to visualize API responses.
It connects directly to the Search API through REST calls and can be used to test queries from a browser.

---

## Technologies

- **Java 17**
- **Javalin 6.x** (REST API framework)
- **MongoDB** (local via Docker or Atlas cloud)
- **Docker Compose** (for service orchestration)
- **Maven** (build and dependency management)

---

## Benchmarking Results

To evaluate the performance of the system, several tests were executed under a 4-core, 32 GB RAM environment.  
The results confirm stable behavior under small and medium workloads.

| Metric | Description |
|--------|--------------|
| **Indexing Throughput** | Shows how many books per second can be processed. |
| **CPU & Memory Utilization** | Resource consumption for Ingestion, Indexing, and Search containers. |
| **Query Latency & Scalability** | Response time and system limits under growing workloads. |

### Figures

## 1. Indexing Throughput

<img width="800" height="500" alt="IndexingThroughput" src="https://github.com/user-attachments/assets/00c195cd-8087-496b-ac14-d374144a4b75" />

## 2. CPU Utilization

<img width="800" height="500" alt="CPU_Utilization" src="https://github.com/user-attachments/assets/dba20166-5fb5-4a44-b6fb-e40e44167fce" />

## 3. Memory Utilization

<img width="800" height="500" alt="Memory_Utilization" src="https://github.com/user-attachments/assets/afa8a4e4-88cc-42ab-91c4-ba6a2d9adb46" />

## 4. Query Latency vs Concurrency

<img width="800" height="500" alt="QueryLatency_vs_Concurrency" src="https://github.com/user-attachments/assets/143f8ba5-9a5d-407e-bb22-aa6caf3a07bd" />

## 5. Scalability Limits (4 cores, 32 GB RAM)

<img width="800" height="500" alt="Scalability_Limits" src="https://github.com/user-attachments/assets/b8e368cb-d469-43c1-9f09-ac7060f45c03" />



---

**The results show that:**
- The ingestion service consumes the most resources (CPU and RAM).  
- Indexing throughput decreases slightly as the dataset grows.  
- Query latency remains stable until around 120 concurrent requests.  
- The system scales well for small and medium workloads.


## Setup and Run

### 1. Clone the repository
```bash
git clone https://github.com/<your_group_name>/stage_2.git
cd stage_2
```

1. Start MongoDB

docker volume create mongodb_data
docker compose up -d

2. Build the project
mvn clean package

3. Run services

# Ingestion (port 7000)
java -cp target/stage-2-1.0.0.jar bigdatastage2.IngestServer

# Indexing (port 7004)
java -cp target/stage-2-1.0.0.jar bigdatastage2.IndexApi

4. Repository structure

```
.
├── control/                        
├── mongo-init/                     
├── src/main/java/bigdatastage2/
│   ├── IngestServer.java           
│   ├── IndexApi.java               
│   ├── SearchApi.java              
│   ├── RepositoryConnection.java 
│   └── resources/
├── docker-compose.yml             
├── pom.xml                         
└── target/
    └── stage-2-1.0.0.jar           
```

5. Authors

- Daniel Nosek 
- Lennart Schega 
- Domen Kac
- Nico Brockmeyer
- Anna Sowińska

