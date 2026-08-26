package task

import (
	"flowgate/internal/models"
)

// IngestTask - единица работы, поставленная в очередь ingest-сервиса.
// Раньше у задачи был собственный context.Context (чтобы не зависеть от
// HTTP-запроса, который её создал), а BulkInsert вызывался на каждую
// задачу отдельно. Теперь воркер объединяет Points из нескольких задач в
// один батч и сам создаёт контекст на момент фактического flush в БД —
// поэтому у задачи больше нет собственного контекста: он был бы неверным
// объектом для тайм-аута, охватывающего сразу несколько запросов.
type IngestTask struct {
	Points []models.TelemetryPoint
	Result chan error
}

// NewIngestTask создаёт задачу. withResult=true, если вызывающая сторона
// хочет дождаться результата обработки (сейчас нигде не используется —
// см. ограничение в handlers.Ingest: при батчинге ошибка одного flush
// относится сразу к нескольким исходным HTTP-запросам, поэтому per-task
// Result для батчей не имеет однозначной семантики).
func NewIngestTask(points []models.TelemetryPoint, withResult bool) *IngestTask {
	t := &IngestTask{Points: points}
	if withResult {
		t.Result = make(chan error, 1)
	}
	return t
}
