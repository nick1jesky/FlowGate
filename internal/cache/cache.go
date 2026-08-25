package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Минимальный интерфейс. Позволяет подменять реализацию без изменения кода вызывающей стороны.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// RedisCache — реализация поверх go-redis.
type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(addr, password string, db int) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     password,
			DB:           db,
			DialTimeout:  2 * time.Second,
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
			PoolSize:     20,
		}),
	}
}

func (r *RedisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisCache) Close() error {
	return r.client.Close()
}

func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := r.client.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (r *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// NoopCache — заглушка на случай, если Redis недоступен при старте.
// Сервис не должен падать целиком из-за недоступности кэша: /query
// в этом режиме просто всегда идёт в БД. Это осознанный компромисс
// "деградация вместо отказа".
type NoopCache struct{}

func (NoopCache) Get(_ context.Context, _ string) ([]byte, bool, error)            { return nil, false, nil }
func (NoopCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }

// Connect пытается поднять RedisCache; при неудаче логирует предупреждение
// и возвращает NoopCache, чтобы остальной сервис продолжил работать.
func Connect(ctx context.Context, addr, password string, db int, logger *logrus.Logger) Cache {
	rc := NewRedisCache(addr, password, db)

	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if err := rc.Ping(pingCtx); err != nil {
		logger.WithError(err).Warn("Redis unavailable at startup, falling back to no-op cache (query caching disabled)")
		return NoopCache{}
	}

	logger.WithField("addr", addr).Info("Redis cache ready")
	return rc
}
