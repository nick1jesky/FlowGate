package task

import (
	"context"
	"time"

	"flowgate/internal/models"
)

// DefaultTimeout — таймаут на обработку задачи воркером. Не зависит от
// жизненного цикла HTTP-запроса, который создал задачу: к моменту, когда
// воркер заберёт её из канала, исходный http-контекст уже мог быть отменён
// (ранее это приводило к тому, что BulkInsert падал с context canceled).
const DefaultTimeout = 5 * time.Second

// IngestTask — единица работы для воркер-пула ingest-сервиса.
type IngestTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	Points []models.TelemetryPoint
	Result chan error
}

// NewIngestTask создаёт задачу с собственным независимым контекстом
// (не унаследованным от HTTP-запроса). withResult=true, если вызывающая
// сторона хочет дождаться результата обработки.
func NewIngestTask(points []models.TelemetryPoint, withResult bool) *IngestTask {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)

	t := &IngestTask{
		ctx:    ctx,
		cancel: cancel,
		Points: points,
	}
	if withResult {
		t.Result = make(chan error, 1)
	}
	return t
}

// Context возвращает контекст задачи — используется воркером для BulkInsert.
func (t *IngestTask) Context() context.Context {
	return t.ctx
}

// Done освобождает ресурсы контекста задачи. Обязательно вызывать после
// того, как воркер закончил обработку (успешно или нет), иначе контексты
// будут копиться до истечения DefaultTimeout.
func (t *IngestTask) Done() {
	t.cancel()
}
