-- Базовая схема для локального запуска / docker-compose.
-- Продовое решение потребует нормальный инструмент миграций (goose/atlas)
-- и партиционирование raw_metrics по времени - см. roadmap в README.

CREATE TABLE IF NOT EXISTS raw_metrics (
    id         BIGSERIAL PRIMARY KEY,
    device_id  TEXT        NOT NULL,
    metric     TEXT        NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    ts         TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Основной паттерн доступа в GetAggregated: WHERE device_id = ... AND ts BETWEEN ...
CREATE INDEX IF NOT EXISTS idx_raw_metrics_device_ts ON raw_metrics (device_id, ts);
