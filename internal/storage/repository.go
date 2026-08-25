package storage

import (
	"context"
	"fmt"
	"time"

	"flowgate/internal/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

	copyCount, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"raw_metrics"},
		[]string{"device_id", "metric", "value", "ts"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("copy from failed: %w", err)
	}
	return copyCount, nil
}

// GetAggregated возвращает агрегированные данные (пока из сырой таблицы)
// В будущем переключим на MV
func (r *Repository) GetAggregated(ctx context.Context, deviceID string, from, to time.Time) ([]models.AggregatedPoint, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			device_id,
			date_trunc('minute', ts) as minute,
			AVG(value) as avg_value
		FROM raw_metrics
		WHERE device_id = $1 AND ts BETWEEN $2 AND $3
		GROUP BY device_id, minute
		ORDER BY minute
	`, deviceID, from, to)
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
