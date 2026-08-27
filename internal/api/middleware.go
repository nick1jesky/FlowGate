package api

import (
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nick1jesky/atlimiter"
)

type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*limiterEntry

	rps            float64
	maxRPS         uint64
	capacityFactor float64
}

type limiterEntry struct {
	limiter  *atlimiter.ATLimiter
	lastSeen time.Time
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	maxRPS := uint64(math.Round(rps))

	capacityFactor := 1.0
	if maxRPS > 0 && burst > 0 {
		capacityFactor = float64(burst) / float64(maxRPS)
	}

	rl := &RateLimiter{
		limiters:       make(map[string]*limiterEntry),
		rps:            rps,
		maxRPS:         maxRPS,
		capacityFactor: capacityFactor,
	}
	go rl.cleanupLoop()
	return rl
}

func (rl *RateLimiter) getLimiter(key string) *atlimiter.ATLimiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	e, ok := rl.limiters[key]
	if !ok {
		e = &limiterEntry{limiter: atlimiter.NewLimiter(rl.maxRPS, rl.capacityFactor)}
		rl.limiters[key] = e
	}
	e.lastSeen = time.Now()
	return e.limiter
}

// cleanupLoop убирает лимитеры клиентов, от которых давно не было запросов.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		for key, e := range rl.limiters {
			if time.Since(e.lastSeen) > 10*time.Minute {
				delete(rl.limiters, key)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	if rl.rps <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		if !rl.getLimiter(c.ClientIP()).Allow() {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

// Простая аутентификация по статическому ключу в заголовке X-API-Key.
func APIKeyAuth(keys []string) gin.HandlerFunc {
	if len(keys) == 0 {
		return func(c *gin.Context) { c.Next() }
	}

	allowed := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		allowed[k] = struct{}{}
	}

	return func(c *gin.Context) {
		key := c.GetHeader("X-API-Key")
		if _, ok := allowed[key]; !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid API key"})
			return
		}
		c.Next()
	}
}
