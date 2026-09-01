package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

type Options struct {
	Addr         string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

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

// Lua скрипт добавляет delta к общему счётчику и сбрасывает его в 0, возвращая значение до сброса.
var addAndSwapScript = redis.NewScript(`
local total = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('SET', KEYS[1], 0)
return total
`)

// AddAndSwap реализует refresher.RedisIncrSwapper.
func (r *RedisCache) AddAndSwap(ctx context.Context, key string, delta int64) (int64, error) {
	return addAndSwapScript.Run(ctx, r.client, []string{key}, delta).Int64()
}

// NoopCache - заглушка на случай, если Redis недоступен при старте.
type NoopCache struct{}

func (NoopCache) Get(_ context.Context, _ string) ([]byte, bool, error)            { return nil, false, nil }
func (NoopCache) Set(_ context.Context, _ string, _ []byte, _ time.Duration) error { return nil }

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
