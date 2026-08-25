package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// NewPool создаёт пул соединений с PostgreSQL. Используется вместо
// одиночного *pgx.Conn, чтобы конкурентные BulkInsert из разных
// ingest-воркеров не боролись за одно и то же соединение (что раньше
// приводило бы к гонке / сериализации всех записей через один conn).
func NewPool(ctx context.Context, databaseURL string, maxConns, minConns int32, logger *logrus.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	cfg.MaxConns = maxConns
	cfg.MinConns = minConns
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 1 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"max_conns": maxConns,
		"min_conns": minConns,
	}).Info("PostgreSQL pool ready")

	return pool, nil
}
