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

func (r *Repository) ListDevices(ctx context.Context) ([]string, error) {
	query, args, err := psql.
		Select("DISTINCT device_id").
		From("agg_metrics_1m").
		OrderBy("device_id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("build query: %w", err)
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var devices []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		devices = append(devices, id)
	}
	return devices, rows.Err()
}
