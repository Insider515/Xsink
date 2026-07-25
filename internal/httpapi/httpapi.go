// httpapi.go — /healthz, /readyz, /metrics.
//
// /readyz — 200 тільки коли pipeline готовий + ping усіх Redis-джерел + Postgres.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// Server — HTTP-адмінка (readiness/liveness/metrics).
type Server struct {
	Addr    string
	Clients map[string]*redis.Client // усі Redis-джерела (ping у /readyz)
	Pool    *pgxpool.Pool
	Log     *slog.Logger

	ready atomic.Bool
	srv   *http.Server
}

// New — конструктор.
func New(addr string, clients map[string]*redis.Client, p *pgxpool.Pool, log *slog.Logger) *Server {
	return &Server{Addr: addr, Clients: clients, Pool: p, Log: log}
}

// SetReady — переключити readyz на 200 після успішного bootstrap пайплайну.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Start — піднімає HTTP у фоновій горутині. addr="" → no-op.
func (s *Server) Start() {
	if s == nil || s.Addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.Handle("/metrics", promhttp.Handler())
	s.srv = &http.Server{
		Addr:              s.Addr,
		Handler:           mux,
		ReadTimeout:       5 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		s.Log.Info("http_start", "addr", s.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Log.Error("http_error", "err", err.Error())
		}
	}()
}

// Shutdown — коректна зупинка HTTP.
func (s *Server) Shutdown(ctx context.Context) {
	if s == nil || s.srv == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(shutdownCtx)
}

// healthz — процес живий, не залежить від залежностей.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// readyz — 200 тільки коли всі залежності доступні і pipeline готовий.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	for name, cli := range s.Clients {
		if err := cli.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis["+name+"]: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	if err := s.Pool.Ping(ctx); err != nil {
		http.Error(w, "postgres: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}
