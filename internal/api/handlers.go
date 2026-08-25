package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"flowgate/internal/models"
	"flowgate/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
)

type Handler struct {
	ingestService *service.IngestService
	logger        *logrus.Logger
	cache         *cache.Cache
}

func NewHandler(ingestService *service.IngestService, logger *logrus.Logger) *Handler {
	c := cache.New(30*time.Second, 1*time.Minute)
	return &Handler{
		ingestService: ingestService,
		logger:        logger,
		cache:         c,
	}
}

func (h *Handler) Ingest(c *gin.Context) {
	var req models.IngestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 100*time.Millisecond)
	defer cancel()

	if err := h.ingestService.Submit(ctx, req); err != nil {
		h.logger.WithError(err).Warn("Ingest submission failed")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ingest queue is full, try again later"})
		return
	}

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

	if cached, found := h.cache.Get(cacheKey); found {
		if item, ok := cached.(*cacheItem); ok {
			age := time.Since(item.FetchedAt)
			if age > 30*time.Second {
				c.Header("X-Cache-Status", "stale")
				h.logger.WithField("cache_key", cacheKey).Debug("Serving stale cache")
				go h.refreshCache(cacheKey, deviceID, from, to)
			} else {
				c.Header("X-Cache-Status", "hit")
			}
			c.JSON(http.StatusOK, item.Data)
			return
		}
	}

	c.Header("X-Cache-Status", "miss")
	data, err := h.ingestService.GetAggregated(c.Request.Context(), deviceID, from, to)
	if err != nil {
		h.logger.WithError(err).Error("Failed to fetch aggregated data")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "database error"})
		return
	}

	// Сохраняем в кэш (храним 60 секунд, но свежим считаем только 30)
	h.cache.Set(cacheKey, &cacheItem{
		Data:      data,
		FetchedAt: time.Now(),
	}, 60*time.Second)

	c.JSON(http.StatusOK, data)
}

// refreshCache – фоновая актуализация кэша
func (h *Handler) refreshCache(key, deviceID string, from, to time.Time) {
	data, err := h.ingestService.GetAggregated(context.Background(), deviceID, from, to)
	if err != nil {
		h.logger.WithError(err).WithField("cache_key", key).Warn("Background cache refresh failed")
		return
	}
	h.cache.Set(key, &cacheItem{
		Data:      data,
		FetchedAt: time.Now(),
	}, 60*time.Second)
	h.logger.WithField("cache_key", key).Debug("Cache refreshed in background")
}

// cacheItem – обёртка для хранения с меткой времени
type cacheItem struct {
	Data      []models.AggregatedPoint `json:"data"`
	FetchedAt time.Time                `json:"fetched_at"`
}
