package task

import (
	"flowgate/internal/models"
)

type IngestTask struct {
	Points []models.TelemetryPoint
	Result chan error
}

func NewIngestTask(points []models.TelemetryPoint, withResult bool) *IngestTask {
	t := &IngestTask{Points: points}
	if withResult {
		t.Result = make(chan error, 1)
	}
	return t
}
