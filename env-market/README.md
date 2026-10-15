# Environment-Market

Микросервисная система оркестрации сред (окружений) в различных инфраструктурах: **Kubernetes**, **VM** и **Bare Metal**.

Прототип реализует событийно-ориентированную (Event-Driven) архитектуру на Go: заказ окружения через Web UI → команда в Kafka → обработка воркером нужного типа → событие с результатом → обновление статуса в PostgreSQL.

---

## Архитектура

```
┌──────────────┐    POST /api/environments     ┌───────────────┐
│  Web UI      │ ────────────────────────────▶ │   core-api    │
│  (браузер)   │ ◀──────────────────────────── │   :8080       │
└──────────────┘    GET  /api/environments     └──────┬────────┘
                                                      │
                                    INSERT status=CREATING
                                                      │
                                                      ▼
                                             ┌─────────────────┐
                                             │   PostgreSQL    │
                                             │  environments   │
                                             └────────┬────────┘
                                                      ▲
                                          UPDATE status=ACTIVE / DELETED
                                                      │
                                                      │
                     produce                          │  consume
                                                      │
              ┌───────────────────────────────────────┴────────────┐
              │                                                    │
              ▼                                                    │
       ┌──────────────┐    env-commands    ┌──────────────┐        │
       │  core-api    │ ─────────────────▶ │    Kafka     │        │
       │  producer    │                    │              │        │
       └──────────────┘                    │ env-events   │        │
                                           └──────┬───────┘        │
                                                  │ env-commands   │
                                                  ▼                │
                                    ┌─────────────────────────┐    │
                                    │  worker-k8s / vm / bm   │    │
                                    │  (единый бинарник,      │    │
                                    │   WORKER_TYPE из env)   │    │
                                    └──────────┬──────────────┘    │
                                               │ produce           │
                                               │ env-events        │
                                               └───────────────────┘
```

### Компоненты

| Компонент       | Роль                                                                 |
|-----------------|----------------------------------------------------------------------|
| `core-api`      | HTTP-сервер (порт 8080), REST API, отдача Web UI, producer `env-commands`, consumer `env-events`, запись/обновление в PostgreSQL |
| `worker-k8s`    | Воркер для Kubernetes (WORKER_TYPE=k8s)                              |
| `worker-vm`     | Воркер для VM (WORKER_TYPE=vm)                                       |
| `worker-bm`     | Воркер для Bare Metal (WORKER_TYPE=bm)                               |
| PostgreSQL      | Хранение метаданных и статусов окружений                             |
| Kafka (KRaft)   | Асинхронный брокер событий (топики `env-commands` и `env-events`)    |
| kafka-ui        | Веб-интерфейс для просмотра топиков, сообщений и consumer-групп      |

### Жизненный цикл окружения

```
CREATING ──▶ ACTIVE
DELETING ──▶ DELETED
```

---

## Технологический стек

- **Go 1.21+**
- **Apache Kafka** (KRaft-режим, без ZooKeeper)
- **PostgreSQL 16**
- **Docker / Docker Compose** (в перспективе — Helm-чарты для Kubernetes)
- Kafka-драйвер: [franz-go](https://github.com/twmb/franz-go) — чистый Go, без CGO
- Postgres-драйвер: [pgx/v5](https://github.com/jackc/pgx)
- UUID: [google/uuid](https://github.com/google/uuid)

---

## Структура проекта

```
.
├── cmd
│   ├── core-api
│   │   ├── main.go          # HTTP-сервер, REST API, producer/consumer
│   │   └── index.html       # Web UI Environment-Market
│   └── worker
│       └── main.go          # Универсальный воркер (k8s / vm / bm)
├── db
│   └── init.sql             # Схема таблицы environments
├── docker-compose.yml       # Манифест локального развёртывания
├── go.mod
└── README.md
```

---

### Запуск

```bash
docker compose up --build
```

### Адреса сервисов

| Сервис           | URL / хост                        |
|------------------|-----------------------------------|
| Environment-Market UI | http://localhost:8080        |
| Kafka UI         | http://localhost:8090             |
| PostgreSQL       | localhost:5432 (`envuser` / `envpass`, БД `envmarket`) |
| Kafka            | localhost:9092                    |

---

## REST API

### Список окружений

```http
GET /api/environments
```

Ответ:

```json
[
  {
    "id": "5b8f6b1e-...",
    "name": "my-app-dev",
    "type": "k8s",
    "status": "ACTIVE",
    "created_at": "2025-01-01T12:00:00Z",
    "updated_at": "2025-01-01T12:00:05Z"
  }
]
```

### Создание окружения

```http
POST /api/environments
Content-Type: application/json

{
  "name": "my-app-dev",
  "type": "k8s"
}
```

`type` ∈ `k8s` | `vm` | `bm`.

Ответ — `202 Accepted` с созданной записью в статусе `CREATING`.

### Удаление окружения

```http
DELETE /api/environments/{id}
```

Ответ — `202 Accepted`, статус в БД меняется на `DELETING`, а по завершении воркера — на `DELETED`.

---

## Формат сообщений Kafka

### Топик `env-commands`

```json
{
  "id": "5b8f6b1e-...",
  "name": "my-app-dev",
  "type": "k8s",
  "action": "CREATE"
}
```

`action` ∈ `CREATE` | `DELETE`.

### Топик `env-events`

```json
{
  "id": "5b8f6b1e-...",
  "type": "k8s",
  "status": "ACTIVE",
  "action": "CREATE",
  "message": "simulated by worker-k8s"
}
```

`status` ∈ `ACTIVE` | `DELETED`.

---

## Схема БД

```sql
CREATE TABLE environments (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('k8s', 'vm', 'bm')),
    status TEXT NOT NULL CHECK (status IN ('CREATING', 'ACTIVE', 'DELETING', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```


