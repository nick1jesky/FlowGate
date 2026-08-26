package main

import (
	"context"
	"fmt"
	"time"

	"flowgate/internal/api"
	"flowgate/internal/cache"
	"flowgate/internal/config"
	"flowgate/internal/metrics"
	"flowgate/internal/refresher"
	"flowgate/internal/server"
	"flowgate/internal/service"
	"flowgate/internal/storage"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func startLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	return logger
}

func connectPostgres(ctx context.Context, cfg *config.Config, logger *logrus.Logger) *pgxpool.Pool {
	pool, err := storage.NewPool(ctx, cfg.DatabaseURL, storage.PoolOptions{
		MaxConns:          cfg.DBMaxConns,
		MinConns:          cfg.DBMinConns,
		MaxConnLifetime:   cfg.DBMaxConnLifetime,
		MaxConnIdleTime:   cfg.DBMaxConnIdleTime,
		HealthCheckPeriod: cfg.DBHealthCheckPeriod,
		ConnectTimeout:    cfg.DBConnectTimeout,
	}, logger)
	if err != nil {
		logger.WithError(err).Fatal("Unable to connect to database")
	}
	return pool
}

func connectRedis(ctx context.Context, cfg *config.Config, logger *logrus.Logger) cache.Cache {
	return cache.Connect(ctx, cache.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		DialTimeout:  cfg.RedisDialTimeout,
		ReadTimeout:  cfg.RedisReadTimeout,
		WriteTimeout: cfg.RedisWriteTimeout,
		PoolSize:     cfg.RedisPoolSize,
	}, cfg.RedisPingTimeout, logger)
}

func createRepository(pool *pgxpool.Pool) *storage.Repository {
	return storage.NewRepository(pool)
}

func createIngestService(repo *storage.Repository, cfg *config.Config, logger *logrus.Logger) *service.IngestService {
	return service.NewIngestService(repo, cfg.WorkersCount, cfg.ChannelBuffer, cfg.IngestTaskTimeout, cfg.IngestBatchMaxSize, cfg.IngestBatchMaxDelay, logger)
}

func createHandler(ingestService *service.IngestService, c cache.Cache, cfg *config.Config, logger *logrus.Logger) *api.Handler {
	return api.NewHandler(ingestService, c, api.Options{
		SubmitTimeout:   cfg.IngestSubmitTimeout,
		CacheTTL:        cfg.CacheTTL,
		CacheStaleAfter: cfg.CacheStaleAfter,
		RefreshTimeout:  cfg.CacheRefreshTimeout,
	}, logger)
}

func createMVRefresher(pool *pgxpool.Pool, cfg *config.Config, rate refresher.RateSource, logger *logrus.Logger) *refresher.Refresher {
	return refresher.New(pool, refresher.Config{
		ViewName:          cfg.MVViewName,
		MinInterval:       cfg.MVRefreshMinInterval,
		MaxInterval:       cfg.MVRefreshMaxInterval,
		HighRateThreshold: cfg.MVRefreshHighRateThreshold,
		LowRateThreshold:  cfg.MVRefreshLowRateThreshold,
	}, rate, logger)
}

func createGinRouter(handler *api.Handler, logger *logrus.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.LoggerWithWriter(logger.Writer()))
	router.Use(gin.Recovery())

	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := router.Group("/api/v1")
	{
		v1.POST("/ingest", handler.Ingest)
		v1.GET("/query", handler.Query)
	}
	return router
}

func main() {
	cfg := config.Load()
	logger := startLogger()
	logger.Info("Starting flowgate Gateway")

	ctx := context.Background()

	pool := connectPostgres(ctx, &cfg, logger)
	defer pool.Close()
	prometheus.MustRegister(metrics.NewPoolCollector(pool))

	redisCache := connectRedis(ctx, &cfg, logger)
	if rc, ok := redisCache.(*cache.RedisCache); ok {
		defer rc.Close()
	}

	repo := createRepository(pool)
	ingestService := createIngestService(repo, &cfg, logger)
	handler := createHandler(ingestService, redisCache, &cfg, logger)
	router := createGinRouter(handler, logger)

	// Адаптивный рефреш материализованного представления - интервал
	// подстраивается под скорость приёма (см. internal/refresher).
	mvRefresher := createMVRefresher(pool, &cfg, ingestService, logger)
	refresherCtx, cancelRefresher := context.WithCancel(context.Background())
	refresherDone := make(chan struct{})
	go func() {
		defer close(refresherDone)
		mvRefresher.Run(refresherCtx)
	}()

	// Запускаем HTTP-сервер с graceful shutdown
	httpServer := server.New(fmt.Sprintf(":%s", cfg.Port), router, logger)
	httpServer.RunAndWait(cfg.HTTPShutdownTimeout)

	// После остановки HTTP-сервера завершаем сервис ингеста
	ctxSvc, cancelSvc := context.WithTimeout(context.Background(), cfg.IngestShutdownTimeout)
	defer cancelSvc()
	if err := ingestService.Shutdown(ctxSvc); err != nil {
		logger.WithError(err).Error("Ingest service shutdown timeout")
	}

	// Останавливаем MV-рефрешер (после воркеров, но до закрытия пула,
	// который используется отложенным defer pool.Close() выше)
	cancelRefresher()
	select {
	case <-refresherDone:
	case <-time.After(2 * time.Second):
		logger.Warn("MV refresher did not stop in time")
	}

	logger.Info("Server stopped completely")
}
