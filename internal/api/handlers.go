package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"flowgate/internal/cache"
	"flowgate/internal/metrics"
	"flowgate/internal/models"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type IngestService interface {
	Submit(ctx context.Context, points []models.TelemetryPoint) error
	GetAggregated(ctx context.Context, deviceID string, from, to time.Time) ([]models.AggregatedPoint, error)
}

type Options struct {
	SubmitTimeout   time.Duration
	CacheTTL        time.Duration
	CacheStaleAfter time.Duration
	RefreshTimeout  time.Duration
}

type Handler struct {
	ingestService IngestService
	logger        *logrus.Logger
	cache         cache.Cache
	opts          Options
}

func NewHandler(ingestService IngestService, c cache.Cache, opts Options, logger *logrus.Logger) *Handler {
	return &Handler{
		ingestService: ingestService,
		logger:        logger,
		cache:         c,
		opts:          opts,
	}
}

func (h *Handler) Ingest(c *gin.Context) {
	var req models.IngestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), h.opts.SubmitTimeout)
	defer cancel()

	if err := h.ingestService.Submit(ctx, req); err != nil {
		metrics.IngestRequestsTotal.WithLabelValues("rejected").Inc()
		h.logger.WithError(err).Warn("Ingest submission failed")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ingest queue is full, try again later"})
		return
	}

	metrics.IngestRequestsTotal.WithLabelValues("accepted").Inc()
	metrics.IngestPointsTotal.Add(float64(len(req)))
	c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
}

func (h *Handler) Query(c *gin.Context) {
	deviceID := c.Query("device_id")
	if deviceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "device_id is required"})
		return
	}

	fromStr := c.Query("from")
	toStr := c.Query("to")
	if fromStr == "" || toStr == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to are required (RFC3339)"})
		return
	}

	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from format, use RFC3339"})
		return
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to format, use RFC3339"})
		return
	}

	cacheKey := fmt.Sprintf("agg:%s:%d:%d", deviceID, from.Unix(), to.Unix())
	ctx := c.Request.Context()
	queryStart := time.Now()

	if raw, found, err := h.cache.Get(ctx, cacheKey); err != nil {
		metrics.CacheRequestsTotal.WithLabelValues("error").Inc()
		h.logger.WithError(err).Warn("Cache read failed, falling back to DB")
	} else if found {
		var item cacheItem
		if unmarshalErr := json.Unmarshal(raw, &item); unmarshalErr == nil {
			age := time.Since(item.FetchedAt)
			if age > h.opts.CacheStaleAfter {
				metrics.CacheRequestsTotal.WithLabelValues("stale").Inc()
				c.Header("X-Cache-Status", "stale")
				h.logger.WithField("cache_key", cacheKey).Debug("Serving stale cache")
				go h.refreshCache(cacheKey, deviceID, from, to)
			} else {
				metrics.CacheRequestsTotal.WithLabelValues("hit").Inc()
				c.Header("X-Cache-Status", "hit")
			}
			metrics.QueryDuration.WithLabelValues("cache").Observe(time.Since(queryStart).Seconds())
			c.JSON(http.StatusOK, item.Data)
			return
		}
		h.logger.WithField("cache_key", cacheKey).Warn("Cache payload corrupted, ignoring")
	}

	metrics.CacheRequestsTotal.WithLabelValues("miss").Inc()
	c.Header("X-Cache-Status", "miss")
	data, err := h.ingestService.GetAggregated(ctx, deviceID, from, to)
	if err != nil {
		h.logger.WithError(err).Error("Failed to fetch aggregated data")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}
	metrics.QueryDuration.WithLabelValues("db").Observe(time.Since(queryStart).Seconds())

	h.storeInCache(ctx, cacheKey, data)
	c.JSON(http.StatusOK, data)
}

// Фоновая актуализация кэша, которая не привязана к ctx http запроса.
func (h *Handler) refreshCache(key, deviceID string, from, to time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), h.opts.RefreshTimeout)
	defer cancel()

	data, err := h.ingestService.GetAggregated(ctx, deviceID, from, to)
	if err != nil {
		h.logger.WithError(err).WithField("cache_key", key).Warn("Background cache refresh failed")
		return
	}
	h.storeInCache(ctx, key, data)
	h.logger.WithField("cache_key", key).Debug("Cache refreshed in background")
}

func (h *Handler) storeInCache(ctx context.Context, key string, data []models.AggregatedPoint) {
	payload, err := json.Marshal(cacheItem{Data: data, FetchedAt: time.Now()})
	if err != nil {
		h.logger.WithError(err).Warn("Failed to marshal cache payload")
		return
	}
	if err := h.cache.Set(ctx, key, payload, h.opts.CacheTTL); err != nil {
		h.logger.WithError(err).WithField("cache_key", key).Warn("Failed to write cache")
	}
}

type cacheItem struct {
	Data      []models.AggregatedPoint `json:"data"`
	FetchedAt time.Time                `json:"fetched_at"`
}
