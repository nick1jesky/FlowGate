-- Партиционирование raw_metrics по месяцам (RANGE по ts).
-- Postgres не поддерживает ALTER TABLE ... PARTITION BY на существующей
-- таблице, поэтому пересоздаём: старая таблица -> новая партиционированная
-- -> перенос данных -> удаление старой. Оборачиваем в транзакцию.
--
-- В этой миграции создано несколько партиций на ближайшие месяцы для
-- демонстрации + DEFAULT-партиция как страховка на случай точек с ts вне
-- заданных диапазонов. В проде создание будущих партиций должно быть
-- автоматизировано (расширение pg_partman или периодический job/cron),
-- иначе однажды упрётесь в границу последней созданной партиции — все
-- новые данные молча поедут в DEFAULT без преимуществ партиционирования
-- (partition pruning по DEFAULT не работает).

BEGIN;

ALTER TABLE raw_metrics RENAME TO raw_metrics_legacy;

CREATE TABLE raw_metrics (
    id          BIGSERIAL,
    device_id   TEXT             NOT NULL,
    metric      TEXT             NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    ts          TIMESTAMPTZ      NOT NULL,
    inserted_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_raw_metrics_device_ts ON raw_metrics (device_id, ts);

-- Партиции на несколько ближайших месяцев (пример; в проде — автоматически).
CREATE TABLE IF NOT EXISTS raw_metrics_y2026m08 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS raw_metrics_y2026m09 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS raw_metrics_y2026m10 PARTITION OF raw_metrics
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');

-- Страховка: точки со временем вне заданных диапазонов (например, из
-- тестов с фиксированными старыми/будущими датами) не должны приводить
-- к ошибке вставки.
CREATE TABLE IF NOT EXISTS raw_metrics_default PARTITION OF raw_metrics DEFAULT;

INSERT INTO raw_metrics (id, device_id, metric, value, ts, inserted_at)
SELECT id, device_id, metric, value, ts, inserted_at FROM raw_metrics_legacy;

DROP TABLE raw_metrics_legacy;

COMMIT;
