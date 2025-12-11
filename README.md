# xp-ohlcv-generator-service

A **microservice** that generates and stores OHLCV (Open, High, Low, Close, Volume) candle data for trading symbols.  
This service is part of the broader **XP (eXPerimental Platform)** ecosystem.

> **Part of the XP Project:** This service is part of the [xp-project](https://github.com/gar2000b/xp-project) workspace, which coordinates multiple XP microservices and infrastructure.

---

## 🚀 Features
- Spring Boot 4.x (modern stack)
- Java 21 runtime
- Self-contained executable **fat JAR**
- REST-ready for future endpoints
- Docker image build + push support
- Actuator endpoints enabled for monitoring

---

## 🧰 Tech Stack

| Component | Choice |
|----------|--------|
| Language | Java 21 |
| Framework | Spring Boot |
| Build Tool | Maven |
| Packaging | Fat JAR |
| Deployment | Docker |

---

## 🛠️ Building & Running

Build:
```bash
mvn clean package
```

Run:
```bash
java -jar target/xp-ohlcv-generator-service-*.jar
```

Service runs at:
```
http://localhost:8083
```

Health endpoint:
```
http://localhost:8083/actuator/health
```

---

## 🐳 Docker Support

Included Dockerfile allows image creation directly from the fat JAR.

Manual build:
```bash
docker build -t gar2000b/xp-ohlcv-generator-service:latest .
```

Run container:
```bash
docker run -p 8083:8083 gar2000b/xp-ohlcv-generator-service:latest
```

---

## 🔁 Release Workflow (Automated Scripts)

This repo includes helper scripts to automate release + Docker publishing.

| Order | Script                  | Purpose |
|------|-------------------------|---------|
| 1️⃣ | `release-mvn-to-git.sh` | Executes Maven release to bump version, tag repo, and build JAR — then triggers Docker build + push |
| 2️⃣ | `build-docker-image.sh` | Builds Docker image using the latest fat JAR |
| 3️⃣ | `push-docker-image.sh`  | Pushes tagged Docker image(s) to Docker Hub |

All run automatically by:
```bash
./release.sh
```

---

## ⚠️ full-reset.sh — Emergency Use Only

Use this script when Maven release fails mid-process and the repo becomes messy.

It:
- Resets repo to `origin/main`
- Deletes untracked files + release leftovers
- Removes `target/`
- Restores clean working state

Run carefully — it **wipes local changes**:
```bash
./full-reset.sh
```

---

## 📌 Status

This is a **research prototype** — design, API, and data model may change rapidly.
