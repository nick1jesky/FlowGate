package server

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

type HTTPServer struct {
	srv    *http.Server
	logger *logrus.Logger
}

func New(addr string, handler *gin.Engine, logger *logrus.Logger) *HTTPServer {
	return &HTTPServer{
		srv: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
		logger: logger,
	}
}

func (s *HTTPServer) RunAndWait(shutdownTimeout time.Duration) {
	go func() {
		s.logger.WithField("addr", s.srv.Addr).Info("Starting HTTP server")
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.WithError(err).Fatal("HTTP server failed")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	s.logger.Info("Received shutdown signal, starting graceful shutdown...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		s.logger.WithError(err).Error("HTTP server shutdown error")
	} else {
		s.logger.Info("HTTP server stopped gracefully")
	}
}
