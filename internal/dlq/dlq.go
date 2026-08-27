package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"flowgate/internal/models"
)

// Потокобезопасный append-only писатель dlq.
type Writer struct {
	mu   sync.Mutex
	path string
}

func NewWriter(path string) (*Writer, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create dlq dir: %w", err)
		}
	}
	return &Writer{path: path}, nil
}

type entry struct {
	FailedAt time.Time               `json:"failed_at"`
	Error    string                  `json:"error"`
	Points   []models.TelemetryPoint `json:"points"`
}

func (w *Writer) Write(points []models.TelemetryPoint, cause error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open dlq file: %w", err)
	}
	defer f.Close()

	line, err := json.Marshal(entry{
		FailedAt: time.Now(),
		Error:    cause.Error(),
		Points:   points,
	})
	if err != nil {
		return fmt.Errorf("marshal dlq entry: %w", err)
	}
	if _, err := f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("write dlq entry: %w", err)
	}
	return nil
}
