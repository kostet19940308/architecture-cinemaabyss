package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct {
	Port               string
	MonolithURL        string
	MoviesServiceURL   string
	EventsServiceURL   string
	GradualMigration   bool
	MoviesMigrationPct int
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseBoolEnv(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func loadConfig(log *slog.Logger) (Config, error) {
	cfg := Config{
		Port:             getenv("PORT", "8000"),
		MonolithURL:      getenv("MONOLITH_URL", ""),
		MoviesServiceURL: getenv("MOVIES_SERVICE_URL", ""),
		EventsServiceURL: getenv("EVENTS_SERVICE_URL", ""),
		GradualMigration: parseBoolEnv(getenv("GRADUAL_MIGRATION", "false")),
	}
	pctStr := getenv("MOVIES_MIGRATION_PERCENT", "0")
	pct, err := strconv.Atoi(pctStr)
	if err != nil {
		return cfg, errors.New("MOVIES_MIGRATION_PERCENT must be an integer 0..100")
	}
	if pct < 0 || pct > 100 {
		return cfg, errors.New("MOVIES_MIGRATION_PERCENT must be between 0 and 100")
	}
	cfg.MoviesMigrationPct = pct

	var missing []string
	if cfg.MonolithURL == "" {
		missing = append(missing, "MONOLITH_URL")
	}
	if cfg.MoviesServiceURL == "" {
		missing = append(missing, "MOVIES_SERVICE_URL")
	}
	if cfg.EventsServiceURL == "" {
		missing = append(missing, "EVENTS_SERVICE_URL")
	}
	if len(missing) > 0 {
		return cfg, errors.New("missing required env: " + strings.Join(missing, ", "))
	}

	log.Info("environment loaded",
		"PORT", cfg.Port,
		"MONOLITH_URL", cfg.MonolithURL,
		"MOVIES_SERVICE_URL", cfg.MoviesServiceURL,
		"EVENTS_SERVICE_URL", cfg.EventsServiceURL,
		"GRADUAL_MIGRATION", cfg.GradualMigration,
		"MOVIES_MIGRATION_PERCENT", cfg.MoviesMigrationPct,
	)
	return cfg, nil
}

func newReverseProxy(target *url.URL, baseLog *slog.Logger) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if ip, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
			req.Header.Set("X-Forwarded-For", ip)
		}
		req.Header.Set("X-Forwarded-Proto", "http")
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Host = target.Host
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		baseLog.Error("proxy upstream error", "err", err)
		http.Error(rw, "upstream error", http.StatusBadGateway)
	}
	return proxy
}

func genReqID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type Router struct {
	log           *slog.Logger
	cfg           Config
	monolithURL   *url.URL
	moviesURL     *url.URL
	eventsURL     *url.URL
	proxyMonolith *httputil.ReverseProxy
	proxyMovies   *httputil.ReverseProxy
	proxyEvents   *httputil.ReverseProxy
	counter       atomic.Uint64
}

func mustParse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

func NewRouter(log *slog.Logger, cfg Config) *Router {
	r := &Router{
		log:         log,
		cfg:         cfg,
		monolithURL: mustParse(cfg.MonolithURL),
		moviesURL:   mustParse(cfg.MoviesServiceURL),
		eventsURL:   mustParse(cfg.EventsServiceURL),
	}
	r.proxyMonolith = newReverseProxy(r.monolithURL, log.With("target", "monolith"))
	r.proxyMovies = newReverseProxy(r.moviesURL, log.With("target", "movies"))
	r.proxyEvents = newReverseProxy(r.eventsURL, log.With("target", "events"))
	return r
}

func (r *Router) chooseMoviesTarget() *httputil.ReverseProxy {
	if !r.cfg.GradualMigration {
		return r.proxyMonolith
	}
	pct := r.cfg.MoviesMigrationPct
	if pct <= 0 {
		return r.proxyMonolith
	}
	if pct >= 100 {
		return r.proxyMovies
	}
	n := r.counter.Add(1)
	if int(n%100) < pct {
		return r.proxyMovies
	}
	return r.proxyMonolith
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	reqID := req.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = genReqID()
	}
	w.Header().Set("X-Request-Id", reqID)
	l := r.log.With("req_id", reqID, "method", req.Method, "path", req.URL.Path, "remote", req.RemoteAddr)
	l.Info("incoming request")

	switch {
	case req.URL.Path == "/health":
		l.Info("health check requested")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	case strings.HasPrefix(req.URL.Path, "/api/events"):
		l.Info("routing to events service", "upstream", r.eventsURL.String())
		r.proxyEvents.ServeHTTP(w, req)
	case strings.HasPrefix(req.URL.Path, "/api/movies"):
		target := r.chooseMoviesTarget()
		up := r.moviesURL
		label := "movies"
		if target == r.proxyMonolith {
			up = r.monolithURL
			label = "monolith"
		}
		l.Info("routing to movies (gradual)", "choice", label, "upstream", up.String(),
			"gradual", r.cfg.GradualMigration, "percent", r.cfg.MoviesMigrationPct)
		target.ServeHTTP(w, req)
	default:
		l.Info("routing to monolith (fallback)", "upstream", r.monolithURL.String())
		r.proxyMonolith.ServeHTTP(w, req)
	}

	ms := time.Duration(math.Round(float64(time.Since(start))/float64(time.Millisecond))) * time.Millisecond
	l.Info("request completed", "duration", ms.String())
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting proxy-service")

	cfg, err := loadConfig(logger)
	if err != nil {
		logger.Error("failed to load env", "error", err)
		os.Exit(1)
	}

	router := NewRouter(logger, cfg)
	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           router,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info("shutdown signal received", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	} else {
		logger.Info("server stopped gracefully")
	}
}
