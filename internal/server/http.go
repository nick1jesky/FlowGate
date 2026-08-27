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

	tlsCertFile string
	tlsKeyFile  string
}

func New(addr string, handler *gin.Engine, certFile, keyFile string, logger *logrus.Logger) *HTTPServer {
	return &HTTPServer{
		srv: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
		logger:      logger,
		tlsCertFile: certFile,
		tlsKeyFile:  keyFile,
	}
}

func (s *HTTPServer) RunAndWait(shutdownTimeout time.Duration) {
	go func() {
		var err error
		if s.tlsCertFile != "" && s.tlsKeyFile != "" {
			s.logger.WithField("addr", s.srv.Addr).Info("Starting HTTPS server")
			err = s.srv.ListenAndServeTLS(s.tlsCertFile, s.tlsKeyFile)
		} else {
			s.logger.WithField("addr", s.srv.Addr).Info("Starting HTTP server")
			err = s.srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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
