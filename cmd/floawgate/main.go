package main

import (
	"context"
	"fmt"

	"flowgate/internal/api"
	"flowgate/internal/cache"
	"flowgate/internal/config"
	"flowgate/internal/server"
	"flowgate/internal/service"
	"flowgate/internal/storage"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
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
	return service.NewIngestService(repo, cfg.WorkersCount, cfg.ChannelBuffer, cfg.IngestTaskTimeout, logger)
}

func createHandler(ingestService *service.IngestService, c cache.Cache, cfg *config.Config, logger *logrus.Logger) *api.Handler {
	return api.NewHandler(ingestService, c, api.Options{
		SubmitTimeout:   cfg.IngestSubmitTimeout,
		CacheTTL:        cfg.CacheTTL,
		CacheStaleAfter: cfg.CacheStaleAfter,
		RefreshTimeout:  cfg.CacheRefreshTimeout,
	}, logger)
}

func createGinRouter(handler *api.Handler, logger *logrus.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.LoggerWithWriter(logger.Writer()))
	router.Use(gin.Recovery())

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

	redisCache := connectRedis(ctx, &cfg, logger)
	if rc, ok := redisCache.(*cache.RedisCache); ok {
		defer rc.Close()
	}

	repo := createRepository(pool)
	ingestService := createIngestService(repo, &cfg, logger)
	handler := createHandler(ingestService, redisCache, &cfg, logger)
	router := createGinRouter(handler, logger)

	httpServer := server.New(fmt.Sprintf(":%s", cfg.Port), router, logger)
	httpServer.RunAndWait(cfg.HTTPShutdownTimeout)

	ctxSvc, cancelSvc := context.WithTimeout(context.Background(), cfg.IngestShutdownTimeout)
	defer cancelSvc()
	if err := ingestService.Shutdown(ctxSvc); err != nil {
		logger.WithError(err).Error("Ingest service shutdown timeout")
	}

	logger.Info("Server stopped completely")
}
