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

---

## Technologies

- **Java 17**
- **Javalin 6.x** (REST API framework)
- **MongoDB** (local via Docker or Atlas cloud)
- **Docker Compose** (for service orchestration)
- **Maven** (build and dependency management)

---

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

# (optional) Search
java -cp target/stage-2-1.0.0.jar bigdatastage2.SearchApi

Test Endpoints
Ingestion
curl http://localhost:7000/status
curl -X POST http://localhost:7000/ingest/1

Indexing
curl http://localhost:7004/status
curl -X POST http://localhost:7004/index/all
curl http://localhost:7004/index/summary

Search (when ready)
curl "http://localhost:<port>/search?q=adventure"

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

