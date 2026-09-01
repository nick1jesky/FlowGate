BEGIN;

-- 1. Создаём партиционированную таблицу raw_metrics
CREATE TABLE IF NOT EXISTS raw_metrics (
    id          BIGSERIAL,
    device_id   TEXT             NOT NULL,
    metric      TEXT             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    ts          TIMESTAMPTZ      NOT NULL,
    inserted_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

-- 2. Индекс для основного запроса по device_id + ts
CREATE INDEX IF NOT EXISTS idx_raw_metrics_device_ts ON raw_metrics (device_id, ts);

-- 3. Партиции на ближайшие месяцы (пример для 2026)
CREATE TABLE IF NOT EXISTS raw_metrics_y2026m08 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE TABLE IF NOT EXISTS raw_metrics_y2026m09 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');

CREATE TABLE IF NOT EXISTS raw_metrics_y2026m10 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

-- Страховочная DEFAULT-партиция (для данных вне заданных диапазонов)
CREATE TABLE IF NOT EXISTS raw_metrics_default PARTITION OF raw_metrics DEFAULT;

-- 4. Материализованное представление с минутными агрегатами
CREATE MATERIALIZED VIEW IF NOT EXISTS agg_metrics_1m AS
SELECT
    device_id,
    date_trunc('minute', ts) AS minute,
    avg(value)               AS avg_value,
    count(*)                 AS sample_count
FROM raw_metrics
GROUP BY device_id, date_trunc('minute', ts)
WITH NO DATA;

-- 5. Уникальный индекс для CONCURRENT REFRESH
CREATE UNIQUE INDEX IF NOT EXISTS idx_agg_metrics_1m_device_minute
    ON agg_metrics_1m (device_id, minute);

-- 6. Первое наполнение (MV пока пуста, обычный REFRESH допустим)
REFRESH MATERIALIZED VIEW agg_metrics_1m;

COMMIT;