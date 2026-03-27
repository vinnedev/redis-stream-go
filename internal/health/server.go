package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Server struct {
	srv    *http.Server
	rdb    *redis.Client
	logger *zap.Logger
}

func NewServer(addr string, rdb *redis.Client, reg http.Handler, log *zap.Logger) *Server {
	mux := http.NewServeMux()

	if log == nil {
		log = zap.NewNop()
	}

	s := &Server{rdb: rdb, logger: log}

	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.Handle("GET /metrics", reg)

	s.srv = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

func NewServerWithPrometheus(addr string, rdb *redis.Client, log *zap.Logger) *Server {
	return NewServer(addr, rdb, promhttp.Handler(), log)
}

func (s *Server) Start() error {
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.rdb.Ping(r.Context()).Err(); err != nil {
		s.logger.Warn("readiness check failed", zap.Int("status", http.StatusServiceUnavailable), zap.Error(err))
		s.writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "error": err.Error()})
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		s.logger.Error("write json response failed", zap.Error(err))
	}
}
