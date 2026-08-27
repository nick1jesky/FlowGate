package refresher

import (
	"testing"
	"time"
)

func newTestRefresher() *Refresher {
	return &Refresher{
		cfg: Config{
			MinInterval:       5 * time.Second,
			MaxInterval:       60 * time.Second,
			HighRateThreshold: 500,
			LowRateThreshold:  50,
		},
	}
}

func TestComputeInterval_HighRateUsesMin(t *testing.T) {
	r := newTestRefresher()
	// 1000 точек за 10с = 100 точек/сек... нужно rate >= 500, возьмём больше.
	got := r.computeInterval(10000, 10*time.Second) // 1000 pts/sec
	if got != r.cfg.MinInterval {
		t.Fatalf("expected MinInterval at high rate, got %v", got)
	}
}

func TestComputeInterval_LowRateUsesMax(t *testing.T) {
	r := newTestRefresher()
	got := r.computeInterval(10, 10*time.Second) // 1 pt/sec, ниже LowRateThreshold
	if got != r.cfg.MaxInterval {
		t.Fatalf("expected MaxInterval at low rate, got %v", got)
	}
}

func TestComputeInterval_MidRateInterpolates(t *testing.T) {
	r := newTestRefresher()
	// (500+50)/2 = 275 pts/sec -> должно быть примерно посередине между Min и Max
	got := r.computeInterval(2750, 10*time.Second) // 275 pts/sec
	mid := (r.cfg.MinInterval + r.cfg.MaxInterval) / 2
	tolerance := 500 * time.Millisecond
	if got < mid-tolerance || got > mid+tolerance {
		t.Fatalf("expected interval near midpoint %v, got %v", mid, got)
	}
}

func TestComputeInterval_ZeroCurrentIntervalFallsBackToMax(t *testing.T) {
	r := newTestRefresher()
	got := r.computeInterval(100, 0)
	if got != r.cfg.MaxInterval {
		t.Fatalf("expected MaxInterval when currentInterval<=0, got %v", got)
	}
}
