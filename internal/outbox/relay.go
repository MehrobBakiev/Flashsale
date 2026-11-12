// Package outbox публикует события из таблицы outbox в Kafka.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
	"github.com/MehrobBakiev/flashsale/internal/storage/postgres"
)

type Store interface {
	ProcessOutbox(ctx context.Context, limit int, publish func(context.Context, []postgres.OutboxRecord) error) (int, error)
	OutboxBacklog(ctx context.Context) (int64, error)
}

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Relay struct {
	store     Store
	writer    Writer
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
}

func NewRelay(store Store, writer Writer, interval time.Duration, batchSize int, logger *slog.Logger) *Relay {
	return &Relay{store: store, writer: writer, interval: interval, batchSize: batchSize, logger: logger}
}

// Run публикует события, пока не отменён ctx. Если пачка была полной — сразу берёт
// следующую, не дожидаясь тика: так relay быстро разгребает накопившийся backlog.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	backoff := r.interval

	for {
		n, err := r.store.ProcessOutbox(ctx, r.batchSize, r.publish)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("outbox relay iteration failed", "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = r.interval
		metrics.OutboxPublished.Add(float64(n))
		if backlog, err := r.store.OutboxBacklog(ctx); err == nil {
			metrics.OutboxBacklog.Set(float64(backlog))
		}
		if n == r.batchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) publish(ctx context.Context, records []postgres.OutboxRecord) error {
	msgs := make([]kafka.Message, 0, len(records))
	for _, rec := range records {
		// event_id = id строки outbox — уникальный и монотонный; потребители используют
		// его для дедупликации.
		var ev domain.OrderEvent
		if err := json.Unmarshal(rec.Payload, &ev); err != nil {
			return fmt.Errorf("decode outbox %d: %w", rec.ID, err)
		}
		ev.EventID = rec.ID
		value, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{
			// Ключ = id заказа: все события одного заказа попадают в одну партицию,
			// значит потребитель увидит их в правильном порядке.
			Key:   []byte(strconv.FormatInt(rec.AggregateID, 10)),
			Value: value,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(rec.EventType)},
				{Key: "event_id", Value: []byte(strconv.FormatInt(rec.ID, 10))},
			},
		})
	}
	return r.writer.WriteMessages(ctx, msgs...)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
