// Package config loads service configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string
	MetricsAddr string

	PostgresDSN string
	RedisAddr   string

	KafkaBrokers []string
	KafkaTopic   string

	// ReservationTTL — сколько живёт бронь, прежде чем товар вернётся в продажу.
	ReservationTTL time.Duration
	// RateLimitPerSecond — сколько попыток брони в секунду разрешено одному пользователю.
	RateLimitPerSecond int
	// IdempotencyTTL — сколько хранится ответ на запрос с Idempotency-Key.
	IdempotencyTTL time.Duration

	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	ExpirerInterval    time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:           getEnv("HTTP_ADDR", ":8080"),
		MetricsAddr:        getEnv("METRICS_ADDR", ":9090"),
		PostgresDSN:        getEnv("POSTGRES_DSN", "postgres://flashsale:flashsale@localhost:5432/flashsale?sslmode=disable"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:       strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "order-events"),
		ReservationTTL:     time.Duration(getInt("RESERVATION_TTL_SEC", 600)) * time.Second,
		RateLimitPerSecond: getInt("RATE_LIMIT_PER_SEC", 5),
		IdempotencyTTL:     time.Duration(getInt("IDEMPOTENCY_TTL_SEC", 86400)) * time.Second,
		OutboxPollInterval: time.Duration(getInt("OUTBOX_POLL_MS", 200)) * time.Millisecond,
		OutboxBatchSize:    getInt("OUTBOX_BATCH_SIZE", 100),
		ExpirerInterval:    time.Duration(getInt("EXPIRER_INTERVAL_MS", 1000)) * time.Millisecond,
	}
	if cfg.ReservationTTL <= 0 {
		return Config{}, fmt.Errorf("RESERVATION_TTL_SEC must be positive")
	}
	if cfg.OutboxBatchSize <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_BATCH_SIZE must be positive")
	}
	return cfg, nil
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
