package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Cache - минимальный интерфейс, нужный хендлерам. Позволяет подменять
// реализацию (Redis / no-op) без изменения кода вызывающей стороны.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// Options - все параметры соединения с Redis, настраиваемые снаружи
// (через переменные среды в config.Config), а не зашитые в код.
type Options struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// RedisCache - реализация поверх go-redis.
type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(opts Options) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr:         opts.Addr,
			Password:     opts.Password,
			DB:           opts.DB,
			DialTimeout:  opts.DialTimeout,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
			PoolSize:     opts.PoolSize,
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

// NoopCache - заглушка на случай, если Redis недоступен при старте.
// Сервис не должен падать целиком из-за недоступности кэша: /query
// в этом режиме просто всегда идёт в БД. Это осознанный компромисс
// "деградация вместо отказа".
type NoopCache struct{}

func (NoopCache) Get(_ context.Context, _ string) ([]byte, bool, error)            { return nil, false, nil }
func (NoopCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }

// Connect пытается поднять RedisCache; при неудаче логирует предупреждение
// и возвращает NoopCache, чтобы остальной сервис продолжил работать.
// pingTimeout - сколько ждём ответа от Redis при старте, тоже настраиваемо.
func Connect(ctx context.Context, opts Options, pingTimeout time.Duration, logger *logrus.Logger) Cache {
	rc := NewRedisCache(opts)

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := rc.Ping(pingCtx); err != nil {
		logger.WithError(err).Warn("Redis unavailable at startup, falling back to no-op cache (query caching disabled)")
		return NoopCache{}
	}

	logger.WithField("addr", opts.Addr).Info("Redis cache ready")
	return rc
}
