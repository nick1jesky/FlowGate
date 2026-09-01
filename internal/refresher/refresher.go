package refresher

import (
	"context"
	"fmt"
	"time"

	"flowgate/internal/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

type RateSource interface {
	PointsIngestedSinceLastCheck() int64
}

type Config struct {
	ViewName string

	MinInterval time.Duration // интервал при высокой нагрузке
	MaxInterval time.Duration // интервал при низкой нагрузке/простое

	HighRateThreshold float64
	LowRateThreshold  float64
}

type Refresher struct {
	pool   *pgxpool.Pool
	cfg    Config
	rate   RateSource
	logger *logrus.Logger
}

func New(pool *pgxpool.Pool, cfg Config, rate RateSource, logger *logrus.Logger) *Refresher {
	return &Refresher{pool: pool, cfg: cfg, rate: rate, logger: logger}
}

func (r *Refresher) Run(ctx context.Context) {
	interval := r.cfg.MaxInterval
	metrics.MVRefreshIntervalSeconds.Set(interval.Seconds())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.logger.WithField("interval", interval).Info("MV refresher started")

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("MV refresher stopped")
			return
		case <-ticker.C:
			start := time.Now()
			if err := r.refresh(ctx); err != nil {
				metrics.MVRefreshErrorsTotal.Inc()
				r.logger.WithError(err).Error("Materialized view refresh failed")
			} else {
				metrics.MVRefreshTotal.Inc()
				metrics.MVRefreshDuration.Observe(time.Since(start).Seconds())
			}

			pointsSinceLastTick := r.rate.PointsIngestedSinceLastCheck()
			newInterval := r.computeInterval(pointsSinceLastTick, interval)
			if newInterval != interval {
				r.logger.WithFields(logrus.Fields{
					"old_interval": interval,
					"new_interval": newInterval,
					"points":       pointsSinceLastTick,
				}).Info("Adjusted MV refresh interval")
				interval = newInterval
				ticker.Reset(interval)
				metrics.MVRefreshIntervalSeconds.Set(interval.Seconds())
			}
		}
	}
}

func (r *Refresher) refresh(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", r.cfg.ViewName))
	return err
}

func (r *Refresher) computeInterval(pointsSinceLastTick int64, currentInterval time.Duration) time.Duration {
	if currentInterval <= 0 {
		return r.cfg.MaxInterval
	}
	rate := float64(pointsSinceLastTick) / currentInterval.Seconds()

	switch {
	case rate >= r.cfg.HighRateThreshold:
		return r.cfg.MinInterval
	case rate <= r.cfg.LowRateThreshold:
		return r.cfg.MaxInterval
	default:
		span := r.cfg.LowRateThreshold - r.cfg.HighRateThreshold
		if span <= 0 {
			return currentInterval
		}
		// frac: 0 у HighRateThreshold, 1 у LowRateThreshold
		frac := (rate - r.cfg.HighRateThreshold) / span
		d := float64(r.cfg.MinInterval) + frac*float64(r.cfg.MaxInterval-r.cfg.MinInterval)
		return time.Duration(d)
	}
}
