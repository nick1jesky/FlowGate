package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// PoolOptions - параметры пула соединений, настраиваемые снаружи
// (через переменные среды в config.Config), а не зашитые в код.
type PoolOptions struct {
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	ConnectTimeout    time.Duration
}

// NewPool создаёт пул соединений с PostgreSQL. Используется вместо
// одиночного *pgx.Conn, чтобы конкурентные BulkInsert из разных
// ingest-воркеров не боролись за одно и то же соединение (что раньше
// приводило бы к гонке / сериализации всех записей через один conn).
func NewPool(ctx context.Context, databaseURL string, opts PoolOptions, logger *logrus.Logger) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = opts.MinConns
	cfg.MaxConnLifetime = opts.MaxConnLifetime
	cfg.MaxConnIdleTime = opts.MaxConnIdleTime
	cfg.HealthCheckPeriod = opts.HealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, opts.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"max_conns": opts.MaxConns,
		"min_conns": opts.MinConns,
	}).Info("PostgreSQL pool ready")

	return pool, nil
}
