-- Базовая схема для локального запуска / docker-compose.
-- Продовое решение потребует нормальный инструмент миграций (goose/atlas)
-- и партиционирование raw_metrics по времени - см. roadmap в README.

CREATE  TABLE IF NOT EXISTS raw_metrics (
    id         BIGSERIAL PRIMARY KEY,
    device_id  TEXT        NOT NULL,
    metric     TEXT        NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    ts         TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Основной паттерн доступа в GetAggregated: WHERE device_id = ... AND ts BETWEEN ...
CREATE INDEX IF NOT EXISTS idx_raw_metrics_device_ts ON raw_metrics (device_id, ts);

-- Минутные агрегаты по устройствам. Отдельная MV вместо прямого запроса
-- к raw_metrics - тяжёлый GROUP BY больше не выполняется на каждый /query,
-- только на плановом REFRESH (см. internal/refresher).

CREATE MATERIALIZED VIEW IF NOT EXISTS agg_metrics_1m AS
SELECT
    device_id,
    date_trunc('minute', ts) AS minute,
    avg(value)               AS avg_value,
    count(*)                 AS sample_count
FROM raw_metrics
GROUP BY device_id, date_trunc('minute', ts)
WITH NO DATA;

-- REFRESH ... CONCURRENTLY требует уникальный индекс на MV - без него
-- рефреш блокирует чтения на всё время пересчёта.
CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_metrics_1m_device_minute
    ON agg_metrics_1m (device_id, minute);

-- Первое наполнение (WITH NO DATA создаёт пустую MV; REFRESH CONCURRENTLY
-- невозможен, пока MV не заполнена хотя бы раз обычным REFRESH).
REFRESH MATERIALIZED VIEW agg_metrics_1m;
