//go:build integration

// Запуск: go test -tags=integration ./internal/storage/...
// Требует локально работающий Docker (testcontainers поднимает
// одноразовый контейнер Postgres на время теста). Не входит в обычный
// `go test ./...`, чтобы CI/локальные прогоны без Docker не падали.
package storage_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"flowgate/internal/models"
	"flowgate/internal/storage"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func discardLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	migrationsDir, err := filepath.Abs("../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}

	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "flowgate",
			"POSTGRES_PASSWORD": "flowgate",
			"POSTGRES_DB":       "flowgate",
		},
		// docker-entrypoint-initdb.d прогоняет все .sql в алфавитном
		// порядке — ровно так же, как в docker-compose.yml.
		Files: []testcontainers.ContainerFile{
			{HostFilePath: migrationsDir, ContainerFilePath: "/docker-entrypoint-initdb.d", FileMode: 0o755},
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}

	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgContainer.Terminate(ctx) })

	host, err := pgContainer.Host(ctx)
	if err != nil {
		t.Fatalf("get container host: %v", err)
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get mapped port: %v", err)
	}

	dsn := "postgres://flowgate:flowgate@" + host + ":" + port.Port() + "/flowgate?sslmode=disable"
	pool, err := storage.NewPool(ctx, dsn, storage.PoolOptions{
		MaxConns:          5,
		MinConns:          1,
		MaxConnLifetime:   time.Minute,
		MaxConnIdleTime:   time.Minute,
		HealthCheckPeriod: time.Minute,
		ConnectTimeout:    10 * time.Second,
	}, discardLogger())
	if err != nil {
		t.Fatalf("connect pool: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func TestRepository_BulkInsertAndGetAggregated(t *testing.T) {
	if os.Getenv("CI_SKIP_DOCKER") != "" {
		t.Skip("Docker not available in this environment")
	}

	pool := setupPostgres(t)
	repo := storage.NewRepository(pool)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Minute)
	points := []models.TelemetryPoint{
		{DeviceID: "dev-1", Metric: "temp", Value: 20.0, Timestamp: now},
		{DeviceID: "dev-1", Metric: "temp", Value: 22.0, Timestamp: now},
	}

	if _, err := repo.BulkInsert(ctx, points); err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}

	// agg_metrics_1m - материализованное представление, заполняется только
	// по REFRESH; в этом тесте рефрешим его напрямую, как это иначе делал
	// бы internal/refresher по расписанию.
	if _, err := pool.Exec(ctx, "REFRESH MATERIALIZED VIEW agg_metrics_1m"); err != nil {
		t.Fatalf("refresh materialized view: %v", err)
	}

	result, err := repo.GetAggregated(ctx, "dev-1", now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatalf("GetAggregated failed: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 aggregated minute bucket, got %d", len(result))
	}
	if result[0].AvgValue != 21.0 {
		t.Fatalf("expected avg_value=21.0, got %v", result[0].AvgValue)
	}
}
