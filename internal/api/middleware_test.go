package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newTestRouter(middlewares ...gin.HandlerFunc) *gin.Engine {
	r := gin.New()
	for _, mw := range middlewares {
		r.Use(mw)
	}
	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })
	return r
}

func TestAPIKeyAuth_NoKeysConfigured_AllowsAll(t *testing.T) {
	r := newTestRouter(APIKeyAuth(nil))
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with auth disabled, got %d", w.Code)
	}
}

func TestAPIKeyAuth_RejectsMissingKey(t *testing.T) {
	r := newTestRouter(APIKeyAuth([]string{"secret"}))
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing key, got %d", w.Code)
	}
}

func TestAPIKeyAuth_AcceptsValidKey(t *testing.T) {
	r := newTestRouter(APIKeyAuth([]string{"secret"}))
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req.Header.Set("X-API-Key", "secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid key, got %d", w.Code)
	}
}

func TestRateLimiter_DisabledWhenRPSNonPositive(t *testing.T) {
	rl := NewRateLimiter(0, 0)
	r := newTestRouter(rl.Middleware())
	for i := range 20 {
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected rate limiting disabled (rps<=0), got %d on request %d", w.Code, i)
		}
	}
}

func TestRateLimiter_BlocksBurstOverflow(t *testing.T) {
	rl := NewRateLimiter(1, 2) // 1 req/sec sustained, burst of 2
	r := newTestRouter(rl.Middleware())

	var lastCode int
	for range 5 {
		req := httptest.NewRequest(http.MethodGet, "/ping", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		lastCode = w.Code
	}
	if lastCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after exceeding burst, got %d", lastCode)
	}
}

func TestRateLimiter_SeparateClientsHaveSeparateBuckets(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	r := newTestRouter(rl.Middleware())

	req1 := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req1.RemoteAddr = "10.0.0.1:1234"
	w1 := httptest.NewRecorder()
	r.ServeHTTP(w1, req1)

	req2 := httptest.NewRequest(http.MethodGet, "/ping", nil)
	req2.RemoteAddr = "10.0.0.2:1234"
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
		t.Fatalf("expected both first requests from distinct IPs to succeed, got %d and %d", w1.Code, w2.Code)
	}

	time.Sleep(10 * time.Millisecond)
}
