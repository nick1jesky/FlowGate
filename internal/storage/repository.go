package storage

import (
	"context"
	"fmt"
	"time"

	"flowgate/internal/metrics"
	"flowgate/internal/models"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) BulkInsert(ctx context.Context, points []models.TelemetryPoint) (int64, error) {
	if len(points) == 0 {
		return 0, nil
	}

	rows := make([][]any, len(points))
	for i, p := range points {
		rows[i] = []any{
			p.DeviceID,
			p.Metric,
			p.Value,
			p.Timestamp,
		}
	}

	start := time.Now()
	copyCount, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"raw_metrics"},
		[]string{"device_id", "metric", "value", "ts"},
		pgx.CopyFromRows(rows),
	)
	metrics.BulkInsertDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.BulkInsertErrorsTotal.Inc()
		return 0, fmt.Errorf("copy from failed: %w", err)
	}
	return copyCount, nil
}

// GetAggregated читает из материализованного представления agg_metrics_1m
// (обновляется адаптивно, см. internal/refresher), а не напрямую из
// raw_metrics - иначе каждый /query заново пересчитывал бы AVG по всем
// сырым точкам в диапазоне.
//
// Важный побочный эффект: данные могут отставать от реального времени на
// величину текущего интервала рефреша (адаптивно от 5с до 60с при
// дефолтных настройках). Для дашборда почти реального времени это
// приемлемо; если нужна секундная точность - эту функцию придётся
// переключить на raw_metrics для "хвоста" последних N минут.
func (r *Repository) GetAggregated(ctx context.Context, deviceID string, from, to time.Time) ([]models.AggregatedPoint, error) {
	query, args, err := psql.
		Select("device_id", "minute", "avg_value").
		From("agg_metrics_1m").
		Where(sq.Eq{"device_id": deviceID}).
		Where(sq.GtOrEq{"minute": from}).
		Where(sq.LtOrEq{"minute": to}).
		OrderBy("minute").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.AggregatedPoint
	for rows.Next() {
		var p models.AggregatedPoint
		if err := rows.Scan(&p.DeviceID, &p.Minute, &p.AvgValue); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}
