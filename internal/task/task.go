package task

import (
	"context"
	"time"

	"flowgate/internal/models"
)

// IngestTask - единица работы для воркер-пула ingest-сервиса.
type IngestTask struct {
	ctx    context.Context
	cancel context.CancelFunc

	Points []models.TelemetryPoint
	Result chan error
}

// NewIngestTask создаёт задачу с собственным независимым контекстом
// (не унаследованным от HTTP-запроса) - иначе к моменту, когда воркер
// заберёт задачу из канала, исходный http-контекст уже может быть отменён
// (BulkInsert будет падать с context canceled). timeout - на сколько
// воркеру отводится на обработку этой конкретной задачи; настраивается
// через IngestTaskTimeout, конфигурация не зашита в код.
// withResult=true, если вызывающая сторона хочет дождаться результата.
func NewIngestTask(points []models.TelemetryPoint, timeout time.Duration, withResult bool) *IngestTask {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

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

// Context возвращает контекст задачи - используется воркером для BulkInsert.
func (t *IngestTask) Context() context.Context {
	return t.ctx
}

// Done освобождает ресурсы контекста задачи. Обязательно вызывать после
// того, как воркер закончил обработку (успешно или нет), иначе контексты
// будут копиться до истечения своего timeout.
func (t *IngestTask) Done() {
	t.cancel()
}
