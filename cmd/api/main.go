// Команда api — HTTP API распродажи.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MehrobBakiev/flashsale/internal/config"
	"github.com/MehrobBakiev/flashsale/internal/httpapi"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
	"github.com/MehrobBakiev/flashsale/internal/service"
	"github.com/MehrobBakiev/flashsale/internal/storage/postgres"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
	"github.com/MehrobBakiev/flashsale/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(logger); err != nil {
		logger.Error("api stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// ctx отменяется по SIGINT/SIGTERM — так docker/kubernetes останавливают контейнер.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := postgres.Migrate(ctx, pool, migrations.FS); err != nil {
		return err
	}
	logger.Info("migrations applied")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, PoolSize: 100})
	defer rdb.Close() //nolint:errcheck
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	repo := postgres.NewRepo(pool)
	stock := redisstore.NewStock(rdb)
	svc := service.NewSaleService(repo, stock, cfg.ReservationTTL, logger)

	if err := svc.WarmUp(ctx); err != nil {
		return err
	}

	handler := httpapi.NewHandler(
		svc,
		redisstore.NewRateLimiter(rdb, cfg.RateLimitPerSecond),
		redisstore.NewIdempotency(rdb, cfg.IdempotencyTTL),
		map[string]httpapi.ReadinessCheck{"postgres": repo.Ping, "redis": stock.Ping},
		logger,
	)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		metrics.Serve(ctx, cfg.MetricsAddr, logger)
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("http server started", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		stop()
		return err
	}

	// Graceful shutdown: перестаём принимать новые соединения и ждём, пока завершатся
	// текущие запросы (но не дольше 10 секунд).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	wg.Wait()
	logger.Info("api stopped")
	return nil
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
