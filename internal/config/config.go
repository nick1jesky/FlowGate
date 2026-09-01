package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL   string
	Port          string
	WorkersCount  int
	ChannelBuffer int

	// PostgreSQL pool
	DBMaxConns          int32
	DBMinConns          int32
	DBMaxConnLifetime   time.Duration
	DBMaxConnIdleTime   time.Duration
	DBHealthCheckPeriod time.Duration
	DBConnectTimeout    time.Duration

	// Redis
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	RedisDialTimeout  time.Duration
	RedisReadTimeout  time.Duration
	RedisWriteTimeout time.Duration
	RedisPoolSize     int
	RedisPingTimeout  time.Duration

	// Ingest pipeline
	IngestSubmitTimeout     time.Duration // сколько ждём места в очереди перед 503 (backpressure)
	IngestTaskTimeout       time.Duration // таймаут на один flush (BulkInsert) батча в БД
	IngestShutdownTimeout   time.Duration // сколько ждём воркеров при graceful shutdown
	IngestBatchMaxSize      int           // сброс батча по достижении этого числа точек
	IngestBatchMaxDelay     time.Duration // сброс батча по таймеру, если точек накопилось меньше
	IngestFlushMaxRetries   int           // сколько раз повторить BulkInsert перед DLQ
	IngestFlushRetryBackoff time.Duration // базовая задержка между повторами (растёт линейно)
	IngestDLQPath           string        // путь к файлу dead-letter очереди (JSON Lines)

	// Query cache
	CacheTTL            time.Duration // сколько всего хранить в Redis
	CacheStaleAfter     time.Duration // после какого возраста отдавать stale + фоново обновлять
	CacheRefreshTimeout time.Duration // таймаут фонового обновления кэша

	HTTPShutdownTimeout time.Duration
	ReadinessTimeout    time.Duration // таймаут пингов зависимостей в /readyz

	// Адаптивное обновление материализованного представления
	MVViewName                 string
	MVRefreshMinInterval       time.Duration // интервал при высокой нагрузке
	MVRefreshMaxInterval       time.Duration // интервал при низкой нагрузке/простое
	MVRefreshHighRateThreshold float64       // точек/сек - выше этого используем MinInterval
	MVRefreshLowRateThreshold  float64       // точек/сек - ниже этого используем MaxInterval
	MVRateRedisKey             string        // ключ в Redis для кластерного счётчика скорости приёма

	// Безопасность API
	APIKeys        []string // непустой список включает проверку X-API-Key; пустой - auth выключен (как раньше)
	RateLimitRPS   float64  // запросов/сек на клиента (по IP), 0 - рейт-лимит выключен
	RateLimitBurst int      // допустимый всплеск сверх RPS
	TLSCertFile    string   // путь к сертификату; пусто - TLS на уровне приложения выключен
	TLSKeyFile     string
}

func Load() Config {
	return Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		Port:          getEnvAsString("PORT", "8080"),
		WorkersCount:  getEnvAsInt("WORKERS_COUNT", 5),
		ChannelBuffer: getEnvAsInt("CHANNEL_BUFFER", 100),

		DBMaxConns:          int32(getEnvAsInt("DB_MAX_CONNS", 20)),
		DBMinConns:          int32(getEnvAsInt("DB_MIN_CONNS", 2)),
		DBMaxConnLifetime:   getEnvAsDuration("DB_MAX_CONN_LIFETIME", 30*time.Minute),
		DBMaxConnIdleTime:   getEnvAsDuration("DB_MAX_CONN_IDLE_TIME", 5*time.Minute),
		DBHealthCheckPeriod: getEnvAsDuration("DB_HEALTH_CHECK_PERIOD", 1*time.Minute),
		DBConnectTimeout:    getEnvAsDuration("DB_CONNECT_TIMEOUT", 5*time.Second),

		RedisAddr:         getEnvAsString("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     os.Getenv("REDIS_PASSWORD"),
		RedisDB:           getEnvAsInt("REDIS_DB", 0),
		RedisDialTimeout:  getEnvAsDuration("REDIS_DIAL_TIMEOUT", 2*time.Second),
		RedisReadTimeout:  getEnvAsDuration("REDIS_READ_TIMEOUT", 500*time.Millisecond),
		RedisWriteTimeout: getEnvAsDuration("REDIS_WRITE_TIMEOUT", 500*time.Millisecond),
		RedisPoolSize:     getEnvAsInt("REDIS_POOL_SIZE", 20),
		RedisPingTimeout:  getEnvAsDuration("REDIS_PING_TIMEOUT", 2*time.Second),

		IngestSubmitTimeout:     getEnvAsDuration("INGEST_SUBMIT_TIMEOUT", 100*time.Millisecond),
		IngestTaskTimeout:       getEnvAsDuration("INGEST_TASK_TIMEOUT", 5*time.Second),
		IngestShutdownTimeout:   getEnvAsDuration("INGEST_SHUTDOWN_TIMEOUT", 10*time.Second),
		IngestBatchMaxSize:      getEnvAsInt("INGEST_BATCH_MAX_SIZE", 500),
		IngestBatchMaxDelay:     getEnvAsDuration("INGEST_BATCH_MAX_DELAY", 200*time.Millisecond),
		IngestFlushMaxRetries:   getEnvAsInt("INGEST_FLUSH_MAX_RETRIES", 3),
		IngestFlushRetryBackoff: getEnvAsDuration("INGEST_FLUSH_RETRY_BACKOFF", 200*time.Millisecond),
		IngestDLQPath:           getEnvAsString("INGEST_DLQ_PATH", "./data/dlq/flowgate-dlq.jsonl"),

		CacheTTL:            getEnvAsDuration("CACHE_TTL", 60*time.Second),
		CacheStaleAfter:     getEnvAsDuration("CACHE_STALE_AFTER", 30*time.Second),
		CacheRefreshTimeout: getEnvAsDuration("CACHE_REFRESH_TIMEOUT", 5*time.Second),

		HTTPShutdownTimeout: getEnvAsDuration("HTTP_SHUTDOWN_TIMEOUT", 5*time.Second),
		ReadinessTimeout:    getEnvAsDuration("READINESS_TIMEOUT", 2*time.Second),

		MVViewName:                 getEnvAsString("MV_VIEW_NAME", "agg_metrics_1m"),
		MVRefreshMinInterval:       getEnvAsDuration("MV_REFRESH_MIN_INTERVAL", 5*time.Second),
		MVRefreshMaxInterval:       getEnvAsDuration("MV_REFRESH_MAX_INTERVAL", 60*time.Second),
		MVRefreshHighRateThreshold: getEnvAsFloat("MV_REFRESH_HIGH_RATE_THRESHOLD", 500),
		MVRefreshLowRateThreshold:  getEnvAsFloat("MV_REFRESH_LOW_RATE_THRESHOLD", 50),
		MVRateRedisKey:             getEnvAsString("MV_RATE_REDIS_KEY", "flowgate:ingest:rate"),

		APIKeys:        getEnvAsStringSlice("API_KEYS", nil),
		RateLimitRPS:   getEnvAsFloat("RATE_LIMIT_RPS", 100),
		RateLimitBurst: getEnvAsInt("RATE_LIMIT_BURST", 200),
		TLSCertFile:    os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:     os.Getenv("TLS_KEY_FILE"),
	}
}

func getEnvAsString(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultValue
}

func getEnvAsFloat(key string, defaultValue float64) float64 {
	if val := os.Getenv(key); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return defaultValue
}

// getEnvAsStringSlice парсит значения вида "key1,key2,key3"
func getEnvAsStringSlice(key string, defaultValue []string) []string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	parts := strings.Split(val, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
