package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type HealthHandler struct {
	db          Pinger
	cache       Pinger // может быть nil (например, активен NoopCache)
	pingTimeout time.Duration
}

func NewHealthHandler(db Pinger, cache Pinger, pingTimeout time.Duration) *HealthHandler {
	return &HealthHandler{db: db, cache: cache, pingTimeout: pingTimeout}
}

func (h *HealthHandler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *HealthHandler) Readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.pingTimeout)
	defer cancel()

	if err := h.db.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "not ready",
			"reason": "database: " + err.Error(),
		})
		return
	}

	if h.cache != nil {
		if err := h.cache.Ping(ctx); err != nil {
			c.JSON(http.StatusOK, gin.H{"status": "ok", "degraded": "cache unavailable"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
