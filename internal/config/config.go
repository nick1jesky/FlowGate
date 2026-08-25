package config

import (
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL   string
	Port          string
	WorkersCount  int
	ChannelBuffer int
	DBMaxConns    int32
	DBMinConns    int32
	RedisAddr     string
	RedisPassword string
	RedisDB       int
}

func Load() Config {
	return Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		Port:          os.Getenv("PORT"),
		WorkersCount:  getEnvAsInt("WORKERS_COUNT", 5),
		ChannelBuffer: getEnvAsInt("CHANNEL_BUFFER", 100),
		// Пул соединений должен покрывать все ingest-воркеры плюс запас
		// под конкурентные /query-запросы — иначе воркеры будут стоять
		// в очереди за соединением друг у друга.
		DBMaxConns:    int32(getEnvAsInt("DB_MAX_CONNS", 20)),
		DBMinConns:    int32(getEnvAsInt("DB_MIN_CONNS", 2)),
		RedisAddr:     getEnvAsString("REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       getEnvAsInt("REDIS_DB", 0),
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
