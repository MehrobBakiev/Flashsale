// Package consumer — обёртка над Kafka consumer group с ретраями и DLQ.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
)

type Handler func(ctx context.Context, ev domain.OrderEvent) error

// ErrPermanent — ошибка, которую бессмысленно ретраить (например, битое сообщение).
var ErrPermanent = errors.New("permanent error")

type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Consumer struct {
	group      string
	reader     Reader
	dlq        Writer
	handle     Handler
	maxRetries int
	logger     *slog.Logger
}

func New(group string, reader Reader, dlq Writer, handle Handler, logger *slog.Logger) *Consumer {
	return &Consumer{group: group, reader: reader, dlq: dlq, handle: handle, maxRetries: 5, logger: logger.With("group", group)}
}

// Run читает сообщения и коммитит offset только после обработки (at-least-once).
// Если обработка не удалась после всех ретраев, сообщение уходит в DLQ и offset
// коммитится — одно «ядовитое» сообщение не должно блокировать всю партицию.
// Поэтому обработчики обязаны быть идемпотентными.
func (c *Consumer) Run(ctx context.Context) {
	defer c.reader.Close() //nolint:errcheck
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("fetch message", "err", err)
			if !sleep(ctx, time.Second) {
				return
			}
			continue
		}

		result := c.process(ctx, msg)
		if ctx.Err() != nil {
			// Остановка посреди обработки: offset не коммитим, сообщение перечитается.
			return
		}
		metrics.ConsumerMessages.WithLabelValues(c.group, result).Inc()

		if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			c.logger.Error("commit offset", "err", err, "offset", msg.Offset)
		}
	}
}

func (c *Consumer) process(ctx context.Context, msg kafka.Message) string {
	var ev domain.OrderEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return c.toDLQ(ctx, msg, errors.Join(ErrPermanent, err))
	}

	backoff := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := c.handle(ctx, ev)
		if err == nil {
			return "ok"
		}
		if errors.Is(err, ErrPermanent) || attempt >= c.maxRetries {
			return c.toDLQ(ctx, msg, err)
		}
		c.logger.Warn("handler failed, retrying", "err", err, "attempt", attempt, "event_id", ev.EventID)
		if !sleep(ctx, backoff) {
			return "aborted"
		}
		backoff *= 2
	}
}

func (c *Consumer) toDLQ(ctx context.Context, msg kafka.Message, err error) string {
	c.logger.Error("sending message to DLQ", "err", err, "offset", msg.Offset, "partition", msg.Partition)
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(append([]kafka.Header{}, msg.Headers...),
			kafka.Header{Key: "dlq_reason", Value: []byte(err.Error())},
			kafka.Header{Key: "dlq_group", Value: []byte(c.group)},
		),
	}
	for {
		werr := c.dlq.WriteMessages(ctx, dlqMsg)
		if werr == nil {
			return "dlq"
		}
		// Не коммитим offset, пока сообщение не легло в DLQ: иначе оно потеряется.
		c.logger.Error("write to DLQ failed", "err", werr)
		if !sleep(ctx, time.Second) {
			return "aborted"
		}
	}
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
