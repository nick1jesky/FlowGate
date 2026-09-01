package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"flowgate/internal/models"

	"github.com/sirupsen/logrus"
)

type fakeRepo struct {
	mu          sync.Mutex
	calls       int
	totalPoints int
	failNextN   atomic.Int32
	lastPoints  []models.TelemetryPoint
}

func (f *fakeRepo) BulkInsert(_ context.Context, points []models.TelemetryPoint) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failNextN.Load() > 0 {
		f.failNextN.Add(-1)
		return 0, errors.New("simulated db error")
	}
	f.totalPoints += len(points)
	f.lastPoints = points
	return int64(len(points)), nil
}

func (f *fakeRepo) GetAggregated(_ context.Context, _ string, _, _ time.Time) ([]models.AggregatedPoint, error) {
	return nil, nil
}

func (f *fakeRepo) ListDevices(_ context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeRepo) snapshot() (calls, totalPoints int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.totalPoints
}

type fakeDLQ struct {
	mu      sync.Mutex
	entries [][]models.TelemetryPoint
}

func (d *fakeDLQ) Write(points []models.TelemetryPoint, _ error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append(d.entries, points)
	return nil
}

func (d *fakeDLQ) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}

func newTestPoints(n int) []models.TelemetryPoint {
	pts := make([]models.TelemetryPoint, n)
	for i := range pts {
		pts[i] = models.TelemetryPoint{DeviceID: "d1", Metric: "temp", Value: float64(i), Timestamp: time.Now()}
	}
	return pts
}

func TestIngestService_FlushesBySize(t *testing.T) {
	repo := &fakeRepo{}
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel) // тише

	svc := NewIngestService(repo, 1, 100, time.Second, 10, time.Hour, RetryConfig{}, nil, logger)

	// 3 запроса по 4 точки = 12 точек, порог батча - 10: должен случиться
	// хотя бы один flush по размеру без ожидания таймера (мы взяли
	// заведомо большой batchMaxDelay = час).
	for range 3 {
		if err := svc.Submit(context.Background(), newTestPoints(4)); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if calls, _ := repo.snapshot(); calls > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	calls, _ := repo.snapshot()
	if calls == 0 {
		t.Fatalf("expected at least one BulkInsert call triggered by batch size, got 0")
	}
}

func TestIngestService_FlushesByTimer(t *testing.T) {
	repo := &fakeRepo{}
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	// Порог по размеру заведомо недостижим (1000), но таймер короткий —
	// единственный способ дождаться flush - это сработавший таймер.
	svc := NewIngestService(repo, 1, 100, time.Second, 1000, 50*time.Millisecond, RetryConfig{}, nil, logger)

	if err := svc.Submit(context.Background(), newTestPoints(3)); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, total := repo.snapshot(); total == 3 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected batch to be flushed by timer within timeout")
}

func TestIngestService_RetriesThenSucceeds(t *testing.T) {
	repo := &fakeRepo{}
	repo.failNextN.Store(2) // первые 2 попытки - ошибка, 3-я - успех

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	svc := NewIngestService(repo, 1, 100, time.Second, 5, 20*time.Millisecond, RetryConfig{MaxRetries: 3, Backoff: 5 * time.Millisecond}, nil, logger)

	if err := svc.Submit(context.Background(), newTestPoints(2)); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, total := repo.snapshot(); total == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected batch to eventually succeed after retries")
}

func TestIngestService_ExhaustedRetriesGoToDLQ(t *testing.T) {
	repo := &fakeRepo{}
	repo.failNextN.Store(100) // всегда ошибка

	dlq := &fakeDLQ{}
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	svc := NewIngestService(repo, 1, 100, time.Second, 5, 20*time.Millisecond, RetryConfig{MaxRetries: 2, Backoff: 5 * time.Millisecond}, dlq, logger)

	if err := svc.Submit(context.Background(), newTestPoints(2)); err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dlq.count() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected batch to land in DLQ after exhausting retries")
}

func TestIngestService_SubmitBackpressure(t *testing.T) {
	repo := &fakeRepo{}
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	// Без воркеров (0), с ёмкостью канала 1 - второй Submit должен упереться
	// в backpressure и вернуть ошибку по истечении переданного контекста.
	svc := NewIngestService(repo, 0, 1, time.Second, 1000, time.Hour, RetryConfig{}, nil, logger)

	if err := svc.Submit(context.Background(), newTestPoints(1)); err != nil {
		t.Fatalf("first submit should succeed (channel has room): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := svc.Submit(ctx, newTestPoints(1)); err == nil {
		t.Fatalf("expected second submit to fail due to backpressure (no workers draining the channel)")
	}
}
