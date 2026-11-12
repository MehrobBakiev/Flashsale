package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/MehrobBakiev/flashsale/internal/domain"
)

// fakeReader отдаёт заранее заданные сообщения, затем блокируется до отмены ctx.
type fakeReader struct {
	mu        sync.Mutex
	msgs      []kafka.Message
	committed []int64
	done      chan struct{}
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.mu.Lock()
	if len(r.msgs) > 0 {
		m := r.msgs[0]
		r.msgs = r.msgs[1:]
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	close(r.done)
	<-ctx.Done()
	return kafka.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range msgs {
		r.committed = append(r.committed, m.Offset)
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

type fakeWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func event(t *testing.T, offset int64) kafka.Message {
	b, err := json.Marshal(domain.OrderEvent{EventID: offset, Type: domain.EventOrderExpired, OrderID: 1, SaleID: 1, UserID: 1})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Offset: offset, Value: b}
}

func run(t *testing.T, reader *fakeReader, dlq *fakeWriter, h Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := New("test", reader, dlq, h, slog.New(slog.NewTextHandler(io.Discard, nil)))
	finished := make(chan struct{})
	go func() { c.Run(ctx); close(finished) }()
	select {
	case <-reader.done:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not drain messages")
	}
	cancel()
	<-finished
}

func TestConsumer_RetriesTransientErrors(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{event(t, 1)}, done: make(chan struct{})}
	dlq := &fakeWriter{}
	attempts := 0
	run(t, reader, dlq, func(context.Context, domain.OrderEvent) error {
		attempts++
		if attempts < 3 {
			return errors.New("redis timeout")
		}
		return nil
	})
	if attempts != 3 || len(dlq.msgs) != 0 || len(reader.committed) != 1 {
		t.Fatalf("attempts=%d dlq=%d committed=%v", attempts, len(dlq.msgs), reader.committed)
	}
}

func TestConsumer_PoisonMessageGoesToDLQ(t *testing.T) {
	bad := kafka.Message{Offset: 1, Value: []byte("{not json")}
	reader := &fakeReader{msgs: []kafka.Message{bad, event(t, 2)}, done: make(chan struct{})}
	dlq := &fakeWriter{}
	handled := 0
	run(t, reader, dlq, func(context.Context, domain.OrderEvent) error { handled++; return nil })

	if len(dlq.msgs) != 1 {
		t.Fatalf("poison message must go to DLQ, dlq=%d", len(dlq.msgs))
	}
	if handled != 1 {
		t.Fatalf("next message must still be processed, handled=%d", handled)
	}
	if len(reader.committed) != 2 {
		t.Fatalf("both offsets must be committed, got %v", reader.committed)
	}
}
