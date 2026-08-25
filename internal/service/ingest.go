package service

import (
	"context"
	"sync"
	"time"

	"flowgate/internal/models"
	"flowgate/internal/storage"
	"flowgate/internal/task"

	"github.com/sirupsen/logrus"
)

type IngestService struct {
	repo     *storage.Repository
	ingestCh chan *task.IngestTask
	wg       sync.WaitGroup
	logger   *logrus.Logger
}

func NewIngestService(repo *storage.Repository, workers int, buffer int, logger *logrus.Logger) *IngestService {
	s := &IngestService{
		repo:     repo,
		ingestCh: make(chan *task.IngestTask, buffer),
		logger:   logger,
	}

	for i := range workers {
		s.wg.Add(1)
		go s.worker(i)
	}

	return s
}

func (s *IngestService) worker(id int) {
	defer s.wg.Done()
	s.logger.WithField("worker_id", id).Info("Ingest worker started")

	for t := range s.ingestCh {
		count, err := s.repo.BulkInsert(t.Context(), t.Points)
		if err != nil {
			s.logger.WithError(err).WithField("points", len(t.Points)).Error("BulkInsert failed")
			if t.Result != nil {
				t.Result <- err
			}
			t.Done()
			continue
		}
		s.logger.WithFields(logrus.Fields{
			"worker_id": id,
			"inserted":  count,
		}).Debug("Batch inserted successfully")

		if t.Result != nil {
			t.Result <- nil
		}
		t.Done()
	}

	s.logger.WithField("worker_id", id).Info("Ingest worker stopped")
}

// Submit ставит задачу в очередь на асинхронную обработку. ctx здесь
// используется ТОЛЬКО для контроля таймаута постановки в канал (backpressure) —
// он НЕ передаётся воркеру, так как HTTP-запрос обычно завершается
// (и его ctx отменяется) задолго до того, как воркер реально обработает
// задачу. У самой задачи — собственный независимый контекст, см. internal/task.
func (s *IngestService) Submit(ctx context.Context, points []models.TelemetryPoint) error {
	t := task.NewIngestTask(points, false)
	select {
	case s.ingestCh <- t:
		return nil
	case <-ctx.Done():
		t.Done()
		return ctx.Err()
	}
}

func (s *IngestService) GetAggregated(ctx context.Context, deviceID string, from, to time.Time) ([]models.AggregatedPoint, error) {
	return s.repo.GetAggregated(ctx, deviceID, from, to)
}

func (s *IngestService) Shutdown(ctx context.Context) error {
	close(s.ingestCh)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
