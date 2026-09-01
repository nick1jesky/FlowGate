package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"flowgate/internal/models"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type fakeIngestService struct {
	submitErr    error
	submitted    []models.TelemetryPoint
	aggregated   []models.AggregatedPoint
	aggregateErr error
}

func (f *fakeIngestService) Submit(_ context.Context, points []models.TelemetryPoint) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = append(f.submitted, points...)
	return nil
}

func (f *fakeIngestService) GetAggregated(_ context.Context, _ string, _, _ time.Time) ([]models.AggregatedPoint, error) {
	if f.aggregateErr != nil {
		return nil, f.aggregateErr
	}
	return f.aggregated, nil
}

func (f *fakeIngestService) ListDevices(_ context.Context) ([]string, error) {
	return nil, nil
}

type fakeCache struct {
	stored map[string][]byte
}

func newFakeCache() *fakeCache { return &fakeCache{stored: map[string][]byte{}} }

func (c *fakeCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := c.stored[key]
	return v, ok, nil
}

func (c *fakeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	c.stored[key] = value
	return nil
}

func newTestHandler(svc *fakeIngestService, c *fakeCache) *Handler {
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return NewHandler(svc, c, nil, Options{
		SubmitTimeout:   time.Second,
		CacheTTL:        time.Minute,
		CacheStaleAfter: 30 * time.Second,
		RefreshTimeout:  time.Second,
	}, logger)
}

func TestHandler_Ingest_Accepted(t *testing.T) {
	svc := &fakeIngestService{}
	h := newTestHandler(svc, newFakeCache())

	r := gin.New()
	r.POST("/ingest", h.Ingest)

	body, _ := json.Marshal([]models.TelemetryPoint{{DeviceID: "d1", Metric: "temp", Value: 1, Timestamp: time.Now()}})
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", w.Code, w.Body.String())
	}
	if len(svc.submitted) != 1 {
		t.Fatalf("expected 1 point submitted, got %d", len(svc.submitted))
	}
}

func TestHandler_Ingest_InvalidBody(t *testing.T) {
	h := newTestHandler(&fakeIngestService{}, newFakeCache())
	r := gin.New()
	r.POST("/ingest", h.Ingest)

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid body, got %d", w.Code)
	}
}

func TestHandler_Ingest_QueueFull(t *testing.T) {
	svc := &fakeIngestService{submitErr: context.DeadlineExceeded}
	h := newTestHandler(svc, newFakeCache())
	r := gin.New()
	r.POST("/ingest", h.Ingest)

	body, _ := json.Marshal([]models.TelemetryPoint{{DeviceID: "d1", Metric: "temp", Value: 1, Timestamp: time.Now()}})
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when Submit fails, got %d", w.Code)
	}
}

func TestHandler_Query_MissingParams(t *testing.T) {
	h := newTestHandler(&fakeIngestService{}, newFakeCache())
	r := gin.New()
	r.GET("/query", h.Query)

	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing device_id, got %d", w.Code)
	}
}

func TestHandler_Query_CacheMissFallsBackToDB(t *testing.T) {
	svc := &fakeIngestService{aggregated: []models.AggregatedPoint{{DeviceID: "d1", AvgValue: 42}}}
	h := newTestHandler(svc, newFakeCache())
	r := gin.New()
	r.GET("/query", h.Query)

	req := httptest.NewRequest(http.MethodGet, "/query?device_id=d1&from=2026-01-01T00:00:00Z&to=2026-01-02T00:00:00Z", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("X-Cache-Status") != "miss" {
		t.Fatalf("expected X-Cache-Status=miss on first request, got %q", w.Header().Get("X-Cache-Status"))
	}

	var got []models.AggregatedPoint
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(got) != 1 || got[0].AvgValue != 42 {
		t.Fatalf("unexpected response body: %+v", got)
	}
}

func TestHandler_Query_CacheHit(t *testing.T) {
	svc := &fakeIngestService{}
	fc := newFakeCache()
	h := newTestHandler(svc, fc)
	r := gin.New()
	r.GET("/query", h.Query)

	from, to := "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"

	// Первый запрос - miss, кладёт данные в кэш через ingestService.
	svc.aggregated = []models.AggregatedPoint{{DeviceID: "d1", AvgValue: 7}}
	req1 := httptest.NewRequest(http.MethodGet, "/query?device_id=d1&from="+from+"&to="+to, nil)
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	// Второй такой же запрос - должен быть hit.
	req2 := httptest.NewRequest(http.MethodGet, "/query?device_id=d1&from="+from+"&to="+to, nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w2.Header().Get("X-Cache-Status") != "hit" {
		t.Fatalf("expected X-Cache-Status=hit on second identical request, got %q", w2.Header().Get("X-Cache-Status"))
	}
}
