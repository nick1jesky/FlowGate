package dlq

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"flowgate/internal/models"
)

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

type Stats struct {
	Count        int       `json:"count"`
	LastFailedAt time.Time `json:"last_failed_at,omitzero"`
	LastError    string    `json:"last_error,omitempty"`
}

func (w *Writer) Stats() (Stats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := os.Open(w.path)
	if errors.Is(err, os.ErrNotExist) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, fmt.Errorf("open dlq file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)

	var stats Stats
	for scanner.Scan() {
		stats.Count++
		var e entry
		if unmarshalErr := json.Unmarshal(scanner.Bytes(), &e); unmarshalErr == nil {
			stats.LastFailedAt = e.FailedAt
			stats.LastError = e.Error
		}
	}
	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("scan dlq file: %w", err)
	}
	return stats, nil
}
