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
}

func Load() Config {
	return Config{
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		Port:          os.Getenv("PORT"),
		WorkersCount:  getEnvAsInt("WORKERS_COUNT", 5),
		ChannelBuffer: getEnvAsInt("CHANNEL_BUFFER", 100),
	}
}

func getEnvAsInt(key string, defaultValue int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultValue
}
