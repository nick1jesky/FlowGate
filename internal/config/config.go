package config

import (
	"os"
	"strconv"
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
	IngestSubmitTimeout   time.Duration // сколько ждём места в очереди перед 503 (backpressure)
	IngestTaskTimeout     time.Duration // таймаут на обработку одной задачи воркером
	IngestShutdownTimeout time.Duration // сколько ждём воркеров при graceful shutdown

	// Query cache
	CacheTTL            time.Duration // сколько всего хранить в Redis
	CacheStaleAfter     time.Duration // после какого возраста отдавать stale + фоново обновлять
	CacheRefreshTimeout time.Duration // таймаут фонового обновления кэша

	HTTPShutdownTimeout time.Duration
}

func Load() Config {
	return Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		Port:          getEnvAsString("PORT", "8080"),
		WorkersCount:  getEnvAsInt("WORKERS_COUNT", 5),
		ChannelBuffer: getEnvAsInt("CHANNEL_BUFFER", 100),

		// Пул соединений должен покрывать все ingest-воркеры плюс запас
		// под конкурентные /query-запросы - иначе воркеры будут стоять
		// в очереди за соединением друг у друга.
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

		IngestSubmitTimeout:   getEnvAsDuration("INGEST_SUBMIT_TIMEOUT", 100*time.Millisecond),
		IngestTaskTimeout:     getEnvAsDuration("INGEST_TASK_TIMEOUT", 5*time.Second),
		IngestShutdownTimeout: getEnvAsDuration("INGEST_SHUTDOWN_TIMEOUT", 10*time.Second),

		CacheTTL:            getEnvAsDuration("CACHE_TTL", 60*time.Second),
		CacheStaleAfter:     getEnvAsDuration("CACHE_STALE_AFTER", 30*time.Second),
		CacheRefreshTimeout: getEnvAsDuration("CACHE_REFRESH_TIMEOUT", 5*time.Second),

		HTTPShutdownTimeout: getEnvAsDuration("HTTP_SHUTDOWN_TIMEOUT", 5*time.Second),
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
