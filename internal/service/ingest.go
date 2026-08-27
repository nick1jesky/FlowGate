package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"flowgate/internal/metrics"
	"flowgate/internal/models"
	"flowgate/internal/task"

	"github.com/sirupsen/logrus"
)

// Repository — всё, что нужно IngestService от слоя хранения. Абстракция
// над *storage.Repository, чтобы юнит-тесты могли подставить фейк вместо
// реального Postgres.
type Repository interface {
	BulkInsert(ctx context.Context, points []models.TelemetryPoint) (int64, error)
	GetAggregated(ctx context.Context, deviceID string, from, to time.Time) ([]models.AggregatedPoint, error)
}

// DeadLetterWriter получает батчи, для которых исчерпаны все попытки
// записи в БД. Реализация — internal/dlq.Writer; может быть nil, тогда
// такие батчи просто теряются (после логирования ошибки), как и раньше.
type DeadLetterWriter interface {
	Write(points []models.TelemetryPoint, cause error) error
}

// RetryConfig — параметры повторных попыток flush при сбое BulkInsert.
type RetryConfig struct {
	MaxRetries int           // 0 = без повторов, сразу в DLQ/лог
	Backoff    time.Duration // базовая задержка; растёт линейно с номером попытки
}

type IngestService struct {
	repo        Repository
	ingestCh    chan *task.IngestTask
	wg          sync.WaitGroup
	logger      *logrus.Logger
	taskTimeout time.Duration

	batchMaxSize  int
	batchMaxDelay time.Duration

	retry RetryConfig
	dlq   DeadLetterWriter

	// pointsIngested — счётчик точек, принятых с последнего опроса
	// PointsIngestedSinceLastCheck (используется адаптивным MV-рефрешером
	// как индикатор текущей нагрузки). Атомарный, т.к. Submit вызывается
	// конкурентно из множества HTTP-хендлеров.
	pointsIngested int64
}

func NewIngestService(repo Repository, workers, buffer int, taskTimeout time.Duration, batchMaxSize int, batchMaxDelay time.Duration, retry RetryConfig, dlq DeadLetterWriter, logger *logrus.Logger) *IngestService {
	metrics.ChannelCapacity.Set(float64(buffer))

	s := &IngestService{
		repo:          repo,
		ingestCh:      make(chan *task.IngestTask, buffer),
		logger:        logger,
		taskTimeout:   taskTimeout,
		batchMaxSize:  batchMaxSize,
		batchMaxDelay: batchMaxDelay,
		retry:         retry,
		dlq:           dlq,
	}

	for i := range workers {
		s.wg.Add(1)
		s.startWorker(i)
	}

	return s
}

// startWorker запускает воркер с автоматическим перезапуском при панике —
// одна паника в BulkInsert/маршалинге не должна навсегда терять одного
// воркера из пула (и тем более не должна ронять весь процесс: без recover
// паника в горутине убивает всё приложение).
func (s *IngestService) startWorker(id int) {
	go func() {
		defer s.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				metrics.WorkerPanicsTotal.Inc()
				s.logger.WithFields(logrus.Fields{
					"worker_id": id,
					"panic":     r,
				}).Error("Ingest worker panicked, restarting")
				s.wg.Add(1)
				s.startWorker(id)
			}
		}()
		s.worker(id)
	}()
}

// worker — накопитель-и-flusher. Объединяет Points из НЕСКОЛЬКИХ входящих
// задач (то есть из нескольких разных HTTP-запросов) в один батч и вызывает
// BulkInsert не на каждую задачу, а по достижении batchMaxSize точек ЛИБО
// по истечении batchMaxDelay с последнего flush — что наступит раньше.
// Это резко снижает число COPY-вызовов под нагрузкой из множества мелких
// запросов, ценой задержки до batchMaxDelay перед тем, как точка реально
// попадёт в БД.
func (s *IngestService) worker(id int) {
	s.logger.WithField("worker_id", id).Info("Ingest worker started")

	buf := make([]models.TelemetryPoint, 0, s.batchMaxSize)

	timer := time.NewTimer(s.batchMaxDelay)
	defer timer.Stop()

	flush := func(trigger string) {
		if len(buf) == 0 {
			return
		}
		s.flush(id, buf)
		metrics.BatchFlushTotal.WithLabelValues(trigger).Inc()
		buf = make([]models.TelemetryPoint, 0, s.batchMaxSize)
	}

	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		select {
		case t, ok := <-s.ingestCh:
			if !ok {
				stopTimer()
				flush("shutdown")
				s.logger.WithField("worker_id", id).Info("Ingest worker stopped")
				return
			}
			metrics.ChannelDepth.Set(float64(len(s.ingestCh)))

			buf = append(buf, t.Points...)
			// Задача сделала своё дело (её точки скопированы в буфер) —
			// дальнейшая судьба этих точек уже не привязана к конкретному
			// HTTP-запросу, который их принёс.
			if t.Result != nil {
				t.Result <- nil
			}

			if len(buf) >= s.batchMaxSize {
				stopTimer()
				flush("size")
				timer.Reset(s.batchMaxDelay)
			}
		case <-timer.C:
			flush("timer")
			timer.Reset(s.batchMaxDelay)
		}
	}
}

// flush выполняет запись батча в БД с ретраями (линейно растущий бэкофф).
// Использует собственный независимый контекст на КАЖДУЮ попытку — не
// привязанный ни к одному из исходных HTTP-запросов, чьи точки попали в
// этот батч, и не растянутый на все попытки сразу (иначе долгий ретрай мог
// бы держать один и тот же контекст открытым неоправданно долго).
// Если все попытки исчерпаны, батч уходит в dead-letter (если настроен)
// вместо молчаливой потери.
func (s *IngestService) flush(workerID int, points []models.TelemetryPoint) {
	var lastErr error

	for attempt := 0; attempt <= s.retry.MaxRetries; attempt++ {
		if attempt > 0 {
			metrics.FlushRetriesTotal.Inc()
			time.Sleep(s.retry.Backoff * time.Duration(attempt))
		}

		ctx, cancel := context.WithTimeout(context.Background(), s.taskTimeout)
		count, err := s.repo.BulkInsert(ctx, points)
		cancel()

		if err == nil {
			metrics.BatchSize.Observe(float64(len(points)))
			s.logger.WithFields(logrus.Fields{
				"worker_id": workerID,
				"inserted":  count,
				"attempt":   attempt,
			}).Debug("Batch flushed")
			return
		}

		lastErr = err
		s.logger.WithError(err).WithFields(logrus.Fields{
			"worker_id": workerID,
			"points":    len(points),
			"attempt":   attempt,
		}).Warn("BulkInsert attempt failed")
	}

	s.logger.WithError(lastErr).WithFields(logrus.Fields{
		"worker_id": workerID,
		"points":    len(points),
	}).Error("Batch flush failed after all retries")

	if s.dlq == nil {
		return
	}
	if dlqErr := s.dlq.Write(points, lastErr); dlqErr != nil {
		s.logger.WithError(dlqErr).Error("Failed to write batch to dead-letter queue — data lost")
		return
	}
	metrics.DLQWritesTotal.Inc()
}

// Submit ставит задачу в очередь на асинхронную обработку. ctx здесь
// используется ТОЛЬКО для контроля таймаута постановки в канал (backpressure) —
// он НЕ передаётся дальше, так как HTTP-запрос обычно завершается
// (и его ctx отменяется) задолго до того, как воркер реально сбросит батч
// в БД. У самого flush — собственный независимый таймаут, см. IngestService.flush.
func (s *IngestService) Submit(ctx context.Context, points []models.TelemetryPoint) error {
	t := task.NewIngestTask(points, false)
	select {
	case s.ingestCh <- t:
		atomic.AddInt64(&s.pointsIngested, int64(len(points)))
		metrics.ChannelDepth.Set(float64(len(s.ingestCh)))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PointsIngestedSinceLastCheck возвращает число точек, принятых с прошлого
// вызова, и атомарно сбрасывает счётчик. Используется internal/refresher
// для адаптации интервала обновления материализованных представлений
// под текущую скорость приёма.
func (s *IngestService) PointsIngestedSinceLastCheck() int64 {
	return atomic.SwapInt64(&s.pointsIngested, 0)
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
