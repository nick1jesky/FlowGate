# FlowGate - Development Guide

## 📌 Обзор проекта

**FlowGate** - это высокопроизводительный пайплайн обработки потоковой телеметрии. Система принимает миллионы событий, асинхронно сохраняет их в PostgreSQL, кэширует агрегаты в Redis и предоставляет быстрый доступ к статистике через HTTP API. Проект написан на Go с использованием Gin, pgx, и Redis, и включает адаптивное обновление материализованных представлений для ускорения аналитических запросов.

**Ключевые возможности:**
- Приём событий (телеметрия) через REST API с немедленным подтверждением (HTTP 202 Accepted).
- Асинхронная запись в PostgreSQL с использованием `COPY` и пула воркеров.
- Кэширование агрегированных данных в Redis с паттерном stale-while-revalidate.
- Автоматическое обновление материализованных представлений (MV) на основе порога новых данных.
- Аналитический CLI на Python для мониторинга и оптимизации.
- Дашборды в Grafana для визуализации метрик и состояния системы.

**Стек технологий:**
- **Go** (1.22+) - основной язык бэкенда.
- **Gin** - HTTP-фреймворк.
- **pgx** - драйвер PostgreSQL.
- **Redis** - кэш.
- **PostgreSQL** - основное хранилище.
- **Python** - CLI-утилиты для аналитики.
- **Grafana** - визуализация.
- **Docker** - контейнеризация и локальная разработка.

---

## 🏗️ Текущая архитектура (на момент написания)

```mermaid
graph TD
    Client[HTTP Client] -->|POST /ingest| IngestHandler[Ingest Handler (Gin)]
    IngestHandler -->|submit task| IngestCh[Buffered Channel]
    IngestCh --> Worker1[Worker 1]
    IngestCh --> Worker2[Worker 2]
    IngestCh --> WorkerN[Worker N]
    Worker1 -->|BulkInsert| PostgreSQL[(PostgreSQL: raw_metrics)]
    Client -->|GET /query| QueryHandler[Query Handler (Gin)]
    QueryHandler -->|Check| Cache[(Redis Cache)]
    Cache -->|stale hit| QueryHandler
    Cache -->|miss| DBQuery[Repository.GetAggregated]
    DBQuery -->|SQL| PostgreSQL
    QueryHandler -->|update| Cache
```

## TODO

* unit-тесты и интеграционные тесты
* Партиционирование для таблицы raw_metrics
* Безопасность API: RateLimiting, авторизация и аутентификация, HTTPS.
* Ретраи или dead-letter queue при BulkInsert точек
* Redis для инкремента счётчика точек и MV-рефрешера
* Swagger
* Обработка паник в воркерах
* В будущем поддерка gRPC