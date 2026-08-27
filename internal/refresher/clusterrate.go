package refresher

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

type RedisIncrSwapper interface {
	AddAndSwap(ctx context.Context, key string, delta int64) (int64, error)
}

type ClusterRateSource struct {
	local  RateSource
	redis  RedisIncrSwapper
	key    string
	logger *logrus.Logger
}

func NewClusterRateSource(local RateSource, redisSwapper RedisIncrSwapper, key string, logger *logrus.Logger) *ClusterRateSource {
	return &ClusterRateSource{local: local, redis: redisSwapper, key: key, logger: logger}
}

func (c *ClusterRateSource) PointsIngestedSinceLastCheck() int64 {
	localDelta := c.local.PointsIngestedSinceLastCheck()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	total, err := c.redis.AddAndSwap(ctx, c.key, localDelta)
	if err != nil {
		c.logger.WithError(err).Warn("Cluster rate sync via Redis failed, falling back to local rate")
		return localDelta
	}
	return total
}
