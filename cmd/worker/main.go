// Команда worker — фоновые процессы: outbox relay, истечение броней, потребители Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"

	"github.com/MehrobBakiev/flashsale/internal/config"
	"github.com/MehrobBakiev/flashsale/internal/consumer"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
	"github.com/MehrobBakiev/flashsale/internal/outbox"
	"github.com/MehrobBakiev/flashsale/internal/storage/postgres"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
	"github.com/MehrobBakiev/flashsale/internal/worker"
	"github.com/MehrobBakiev/flashsale/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
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

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close() //nolint:errcheck

	repo := postgres.NewRepo(pool)
	stock := redisstore.NewStock(rdb)

	// RequireAll: запись считается успешной, только когда её подтвердили все in-sync реплики.
	// Без этого при падении лидера уже «опубликованное» событие могло бы потеряться.
	eventsWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
	defer eventsWriter.Close() //nolint:errcheck

	dlqWriter := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.KafkaTopic + ".dlq",
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer dlqWriter.Close() //nolint:errcheck

	newReader := func(group string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.KafkaBrokers,
			GroupID:     group,
			Topic:       cfg.KafkaTopic,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafka.FirstOffset,
		})
	}

	// Две независимые consumer group читают один топик: каждая получает все события.
	stockReleaser := consumer.New("stock-releaser", newReader("stock-releaser"), dlqWriter,
		worker.ReleaseStockHandler(stock, logger), logger)
	notifier := consumer.New("notifier", newReader("notifier"), dlqWriter,
		worker.NotifyHandler(repo, logger), logger)

	relay := outbox.NewRelay(repo, eventsWriter, cfg.OutboxPollInterval, cfg.OutboxBatchSize, logger)
	expirer := worker.NewExpirer(repo, cfg.ExpirerInterval, logger)

	tasks := map[string]func(context.Context){
		"outbox-relay":   relay.Run,
		"expirer":        expirer.Run,
		"stock-releaser": stockReleaser.Run,
		"notifier":       notifier.Run,
		"metrics":        func(ctx context.Context) { metrics.Serve(ctx, cfg.MetricsAddr, logger) },
	}

	var wg sync.WaitGroup
	for name, task := range tasks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("task started", "task", name)
			task(ctx)
			logger.Info("task stopped", "task", name)
		}()
	}

	<-ctx.Done()
	logger.Info("shutting down, waiting for tasks")
	wg.Wait()
	logger.Info("worker stopped")
	return nil
}
