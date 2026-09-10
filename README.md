
# Uber-Style Ride Dispatch Platform

<p align="center">
  <strong>A distributed, event-driven ride dispatch backend inspired by Uber.</strong>
</p>

<p align="center">
  Real-time driver discovery · Intelligent matching · Kafka events · Redis Geospatial · Spring Boot · Docker
</p>


---

## ✦ Overview

A backend system that models the core dispatch workflow of a ride-hailing platform.

Instead of relying on a single monolithic service, the application separates **driver location tracking**, **ride management**, and **driver matching** into independent services.

When a rider requests a ride, the request travels through an asynchronous Kafka pipeline, nearby drivers are discovered using Redis Geospatial queries, and the matching service ranks available drivers before assigning the best candidate.

```text
┌──────────────┐
│  Driver App  │
└──────┬───────┘
       │ location update
       ▼
┌────────────────────┐
│  Location Service  │
└─────────┬──────────┘
          │
          ▼
     ┌─────────┐
     │  Redis  │
     │ GEO Data│
     └─────────┘


┌──────────────┐
│   Rider App  │
└──────┬───────┘
       │ request ride
       ▼
┌─────────────────┐
│   Ride Service  │
└────────┬────────┘
         │ ride.requested
         ▼
      ┌───────┐
      │ Kafka │
      └───┬───┘
          │
          ▼
┌─────────────────────┐
│  Matching Service   │
└─────────┬───────────┘
          │
          │ find nearby drivers
          ▼
┌────────────────────┐
│  Location Service  │
└─────────┬──────────┘
          │
          ▼
  Driver Scoring
  Distance + Rating
          │
          ▼
      ┌───────┐
      │ Kafka │
      └───┬───┘
          │ ride.matched
          ▼
┌─────────────────┐
│   Ride Service  │
└─────────────────┘
          │
          ▼
     Ride ACCEPTED
```

---

## ⚡ What The System Does

### 📍 Real-Time Driver Tracking

Driver locations are stored using **Redis Geospatial indexes**, allowing the system to efficiently search for drivers within a geographic radius.

```text
Driver
   │
   │ latitude + longitude
   ▼
Location Service
   │
   ▼
Redis GEO
```

---

### 🎯 Intelligent Driver Matching

When a ride request arrives, the matching service:

1. Retrieves nearby drivers.
2. Calculates driver distance.
3. Considers driver rating.
4. Calculates a weighted score.
5. Selects the highest-ranked driver.
6. Publishes a `ride.matched` event.

The system therefore doesn't simply choose the closest driver—it can balance **proximity and driver quality**.

---

### ⚡ Event-Driven Ride Processing

Ride requests and matches are communicated asynchronously through Kafka.

```text
ride.requested
      │
      ▼
Matching Service
      │
      ▼
Driver Selection
      │
      ▼
ride.matched
```

This keeps ride management and driver matching decoupled.

---

## 🧩 Service Architecture

| Service                 |  Port  | Responsibility                                    |
| :---------------------- | :----: | :------------------------------------------------ |
| 📍 **Location Service** | `8082` | Driver locations and Redis Geospatial operations  |
| 🚕 **Ride Service**     | `8083` | Ride creation, state transitions and ride history |
| 🎯 **Matching Service** | `8084` | Driver discovery, scoring and assignment          |

### Infrastructure

| Component          | Purpose                            |
| :----------------- | :--------------------------------- |
| **Redis**          | Geospatial driver location storage |
| **Kafka**          | Asynchronous event communication   |
| **Zookeeper**      | Kafka coordination                 |
| **MySQL**          | Persistent ride/application data   |
| **Docker Compose** | Local infrastructure orchestration |

---

# 🔄 Ride Lifecycle

Every ride progresses through a controlled state machine:

```text
REQUESTED
    │
    ▼
 MATCHING
    │
    ▼
 ACCEPTED
    │
    ▼
  STARTED
    │
    ▼
 COMPLETED
```

### State meanings

| State       | Meaning                          |
| :---------- | :------------------------------- |
| `REQUESTED` | Rider has requested a ride       |
| `MATCHING`  | System is searching for a driver |
| `ACCEPTED`  | A driver has been assigned       |
| `STARTED`   | Ride has begun                   |
| `COMPLETED` | Ride has finished                |

This prevents arbitrary ride transitions and keeps the ride lifecycle predictable.

---

# 🧠 Driver Matching

The matching service combines multiple signals when ranking drivers.

### Example scoring model

```text
Score =
    Distance Weight × Distance Score
  + Rating Weight   × Rating Score
```

Conceptually:

```text
                 Ride Request
                      │
                      ▼
             Nearby Driver Search
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Driver 1    Driver 2    Driver 3
          │           │           │
          ▼           ▼           ▼
       Distance    Distance    Distance
       Rating      Rating      Rating
          │           │           │
          └───────────┼───────────┘
                      ▼
                 Score Drivers
                      │
                      ▼
                Best Candidate
```

This creates a foundation for more sophisticated dispatch strategies later.

---

# 🗺️ Redis Geospatial

Driver coordinates are stored in Redis using a GEO index.

### Add a driver

```redis
GEOADD drivers:locations 77.5946 12.9716 "driver:1"
```

### Find nearby drivers

```redis
GEOSEARCH drivers:locations
FROMMEMBER "driver:1"
BYRADIUS 5 km
ASC
COUNT 5
```

### Inspect stored drivers

```bash
docker exec -it redis-geo redis-cli
```

```redis
ZRANGE drivers:locations 0 -1
```

### Get a driver's position

```redis
GEOPOS drivers:locations "driver:1"
```

### Calculate distance

```redis
GEODIST drivers:locations "driver:1" "driver:2" km
```

---

# 📨 Kafka Event Flow

Kafka acts as the communication layer between ride processing and driver matching.

### Ride requested

```text
Ride Service
     │
     │ publish
     ▼
ride.requested
     │
     ▼
Kafka
     │
     ▼
Matching Service
```

### Ride matched

```text
Matching Service
     │
     │ publish
     ▼
ride.matched
     │
     ▼
Kafka
     │
     ▼
Ride Service
     │
     ▼
Update ride → ACCEPTED
```

This allows the matching service to operate independently from the ride service.

---

# 🛠️ Tech Stack

### Backend

* Java
* Spring Boot
* Spring Web
* Maven

### Distributed Systems

* Apache Kafka
* Redis
* REST-based service communication

### Data

* MySQL
* Redis Geospatial

### Infrastructure

* Docker
* Docker Compose

---

# 🚀 Getting Started

## Prerequisites

Make sure you have:

* Java 17+
* Maven
* Docker
* Docker Compose

---

## 1. Start Infrastructure

From the project root:

```bash
docker-compose up -d
```

This starts:

```text
Redis
MySQL
Kafka
Zookeeper
```

Give Kafka a few seconds to initialize before starting the Spring services.

---

## 2. Start Location Service

```bash
cd location-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8082
```

---

## 3. Start Ride Service

```bash
cd ride-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8083
```

---

## 4. Start Matching Service

```bash
cd matching-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8084
```

---

# 🧪 End-to-End Demo

## Step 1 — Register Driver Locations

Send driver coordinates to the Location Service.

```http
POST http://localhost:8082/api/v1/locations/drivers/update
```

```json
{
  "driverId": "driver:1",
  "latitude": 12.9716,
  "longitude": 77.5946
}
```

Add additional drivers:

```json
{
  "driverId": "driver:2",
  "latitude": 12.9800,
  "longitude": 77.5800
}
```

```json
{
  "driverId": "driver:3",
  "latitude": 12.9600,
  "longitude": 77.6100
}
```

---

## Step 2 — Request A Ride

```http
POST http://localhost:8083/api/v1/rides/request
```

```json
{
  "riderId": "rider:1",
  "pickupLatitude": 12.9716,
  "pickupLongitude": 77.5946,
  "pickupAddress": "MG Road, Bangalore",
  "dropLatitude": 12.9352,
  "dropLongitude": 77.6245,
  "dropAddress": "Koramangala, Bangalore"
}
```

The request creates the ride and publishes:

```text
ride.requested
```

---

## Step 3 — Driver Matching

The Matching Service consumes the Kafka event and:

```text
Ride Request
     ↓
Find nearby drivers
     ↓
Calculate scores
     ↓
Select driver
     ↓
Publish ride.matched
```

The Ride Service consumes the match event and updates the ride.

Expected state:

```text
ACCEPTED
```

---

## Step 4 — Check Ride

```http
GET http://localhost:8083/api/v1/rides/{rideId}
```

The response contains the assigned driver and current ride status.

---

## Step 5 — Start Ride

```http
PUT http://localhost:8083/api/v1/rides/{rideId}/start
```

State:

```text
ACCEPTED → STARTED
```

---

## Step 6 — Complete Ride

```http
PUT http://localhost:8083/api/v1/rides/{rideId}/complete
```

State:

```text
STARTED → COMPLETED
```

---

## Step 7 — Rider History

```http
GET http://localhost:8083/api/v1/rides/rider/rider:1
```

Returns the rider's completed and previous rides.

---

# 🔍 What This Project Demonstrates

### Architecture

* Microservice-style service separation
* Event-driven communication
* Service-to-service REST communication
* Asynchronous processing

### Redis

* `GEOADD`
* `GEOSEARCH`
* `GEOPOS`
* `GEODIST`
* Geographic proximity queries

### Kafka

* Producers
* Consumers
* Topics
* Asynchronous ride events
* Service decoupling

### Backend Engineering

* REST APIs
* Ride state machines
* Business logic
* Driver ranking
* Persistent data management

### Infrastructure

* Dockerized infrastructure
* Multi-service local development
* Environment-based service configuration

---

# 📌 Project Structure

```text
uber-application/
│
├── location-service/
│   ├── src/
│   └── pom.xml
│
├── ride-service/
│   ├── src/
│   └── pom.xml
│
├── matching-service/
│   ├── src/
│   └── pom.xml
│
├── docker-compose.yml
│
└── README.md
```

---

# 🎯 Why This Architecture?

The system intentionally separates responsibilities instead of putting everything into one application.

```text
Location Service
      │
      │
      ▼
 Redis GEO
      │
      │
      ▼
Matching Service ◄──── Kafka ────► Ride Service
      │                              │
      └──────── Driver Match ────────┘
```

This makes each service independently responsible for a specific part of the ride-dispatch workflow while Kafka handles asynchronous events between services.

---

# 🔮 Future Improvements

The current system provides the foundation for a larger ride-hailing backend.

Potential extensions include:

* Driver availability states
* Driver acceptance/rejection
* Automatic rematching
* Surge pricing
* ETA calculation
* WebSocket-based live ride tracking
* Authentication and authorization
* Distributed rate limiting
* Dead-letter queues for failed Kafka events
* Idempotent event processing
* Observability with metrics and tracing
* Horizontal service scaling
* Kubernetes deployment

---

## ⭐ Core Takeaway

This project focuses on the backend problems behind a ride-hailing platform:

> **How do you find nearby drivers, choose the best one, process ride events asynchronously, and maintain a reliable ride lifecycle across multiple services?**

The implementation uses **Spring Boot, Redis Geospatial, Kafka, MySQL, REST communication, and Docker** to model those core challenges in a distributed architecture.
