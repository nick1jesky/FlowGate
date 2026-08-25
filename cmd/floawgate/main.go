package main

import (
	"context"
	"fmt"
	"time"

	"flowgate/internal/api"
	"flowgate/internal/config"
	"flowgate/internal/server"
	"flowgate/internal/service"
	"flowgate/internal/storage"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

func startLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	return logger
}

func connectPostgres(ctx context.Context, cfg *config.Config, logger *logrus.Logger) *pgx.Conn {
	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.WithError(err).Fatal("Unable to connect to database")
	}
	return conn
}

func createRepository(conn *pgx.Conn) *storage.Repository {
	return storage.NewRepository(conn)
}

func createIngestService(repo *storage.Repository, cfg *config.Config, logger *logrus.Logger) *service.IngestService {
	return service.NewIngestService(repo, cfg.WorkersCount, cfg.ChannelBuffer, logger)
}

func createHandler(ingestService *service.IngestService, logger *logrus.Logger) *api.Handler {
	return api.NewHandler(ingestService, logger)
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
	conn := connectPostgres(ctx, &cfg, logger)
	defer conn.Close(ctx)

	repo := createRepository(conn)
	ingestService := createIngestService(repo, &cfg, logger)
	handler := createHandler(ingestService, logger)
	router := createGinRouter(handler, logger)

	httpServer := server.New(fmt.Sprintf(":%s", cfg.Port), router, logger)
	httpServer.RunAndWait(5 * time.Second)

	ctxSvc, cancelSvc := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSvc()
	if err := ingestService.Shutdown(ctxSvc); err != nil {
		logger.WithError(err).Error("Ingest service shutdown timeout")
	}

	logger.Info("Server stopped completely")
}
