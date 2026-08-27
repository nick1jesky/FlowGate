package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Ingest pipeline
	IngestRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "requests_total",
		Help:      "Total /ingest HTTP requests, by outcome (accepted, rejected).",
	}, []string{"status"})

	IngestPointsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "points_total",
		Help:      "Total telemetry points accepted for ingestion.",
	})

	BulkInsertDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "bulk_insert_duration_seconds",
		Help:      "Duration of BulkInsert (COPY) calls against PostgreSQL.",
		Buckets:   prometheus.DefBuckets,
	})

	BulkInsertErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "bulk_insert_errors_total",
		Help:      "Total failed BulkInsert calls.",
	})

	ChannelDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "channel_depth",
		Help:      "Current number of tasks waiting in the ingest channel.",
	})

	ChannelCapacity = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "channel_capacity",
		Help:      "Configured capacity (buffer size) of the ingest channel.",
	})

	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "batch_size",
		Help:      "Number of points flushed to PostgreSQL per batch.",
		Buckets:   []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
	})

	BatchFlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "batch_flush_total",
		Help:      "Total batch flushes, by trigger (size, timer, shutdown).",
	}, []string{"trigger"})

	FlushRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "flush_retries_total",
		Help:      "Total retry attempts for a failed batch flush.",
	})

	DLQWritesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "dlq_writes_total",
		Help:      "Total batches written to the dead-letter queue after exhausting retries.",
	})

	WorkerPanicsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "ingest",
		Name:      "worker_panics_total",
		Help:      "Total recovered panics in ingest workers (worker is restarted after each).",
	})

	// Query path / cache
	CacheRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "query",
		Name:      "cache_requests_total",
		Help:      "Cache lookups on /query, by result (hit, stale, miss, error).",
	}, []string{"result"})

	QueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "flowgate",
		Subsystem: "query",
		Name:      "duration_seconds",
		Help:      "Duration of /query requests, by data source (cache, db).",
		Buckets:   prometheus.DefBuckets,
	}, []string{"source"})

	// Materialized view refresh
	MVRefreshTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "mv",
		Name:      "refresh_total",
		Help:      "Total successful materialized view refreshes.",
	})

	MVRefreshErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "flowgate",
		Subsystem: "mv",
		Name:      "refresh_errors_total",
		Help:      "Total failed materialized view refresh attempts.",
	})

	MVRefreshDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "flowgate",
		Subsystem: "mv",
		Name:      "refresh_duration_seconds",
		Help:      "Duration of REFRESH MATERIALIZED VIEW CONCURRENTLY.",
		Buckets:   prometheus.DefBuckets,
	})

	MVRefreshIntervalSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "flowgate",
		Subsystem: "mv",
		Name:      "refresh_interval_seconds",
		Help:      "Current adaptive interval between MV refreshes.",
	})
)

type PoolCollector struct {
	pool     *pgxpool.Pool
	acquired *prometheus.Desc
	idle     *prometheus.Desc
	total    *prometheus.Desc
	maxConns *prometheus.Desc
}

func NewPoolCollector(pool *pgxpool.Pool) *PoolCollector {
	return &PoolCollector{
		pool:     pool,
		acquired: prometheus.NewDesc("flowgate_db_pool_acquired_conns", "Connections currently in use.", nil, nil),
		idle:     prometheus.NewDesc("flowgate_db_pool_idle_conns", "Connections currently idle.", nil, nil),
		total:    prometheus.NewDesc("flowgate_db_pool_total_conns", "Total connections (acquired+idle+constructing).", nil, nil),
		maxConns: prometheus.NewDesc("flowgate_db_pool_max_conns", "Configured maximum connections.", nil, nil),
	}
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquired
	ch <- c.idle
	ch <- c.total
	ch <- c.maxConns
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	stat := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.acquired, prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stat.MaxConns()))
}
