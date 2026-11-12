package redisstore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MehrobBakiev/flashsale/internal/domain"
)

func newTestStock(t *testing.T) (*Stock, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), PoolSize: 64})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStock(rdb), rdb
}

// Главный тест проекта: 1000 покупателей одновременно бьются за 100 единиц.
// Должно быть ровно 100 успешных резервов и 0 в остатке — без оверселла.
func TestReserve_ConcurrentNoOversell(t *testing.T) {
	ctx := context.Background()
	stock, _ := newTestStock(t)
	const units, buyers = 100, 1000

	if _, err := stock.InitIfAbsent(ctx, 1, units, time.Unix(0, 0), nil); err != nil {
		t.Fatal(err)
	}

	var ok, soldOut atomic.Int64
	var wg sync.WaitGroup
	for u := int64(1); u <= buyers; u++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := stock.Reserve(ctx, 1, u, time.Now())
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrSoldOut):
				soldOut.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != units {
		t.Fatalf("reserved %d, want %d", ok.Load(), units)
	}
	if soldOut.Load() != buyers-units {
		t.Fatalf("sold out %d, want %d", soldOut.Load(), buyers-units)
	}
	if avail, _ := stock.Available(ctx, 1); avail != 0 {
		t.Fatalf("available %d, want 0", avail)
	}
}

func TestReserve_Rules(t *testing.T) {
	ctx := context.Background()
	stock, _ := newTestStock(t)
	now := time.Now()

	if err := stock.Reserve(ctx, 1, 10, now); !errors.Is(err, ErrNotLoaded) {
		t.Fatalf("want ErrNotLoaded, got %v", err)
	}

	future := now.Add(time.Hour)
	if _, err := stock.InitIfAbsent(ctx, 2, 5, future, nil); err != nil {
		t.Fatal(err)
	}
	if err := stock.Reserve(ctx, 2, 10, now); !errors.Is(err, domain.ErrSaleNotStarted) {
		t.Fatalf("want ErrSaleNotStarted, got %v", err)
	}

	if _, err := stock.InitIfAbsent(ctx, 3, 5, now.Add(-time.Minute), []int64{42}); err != nil {
		t.Fatal(err)
	}
	if err := stock.Reserve(ctx, 3, 42, now); !errors.Is(err, domain.ErrAlreadyReserved) {
		t.Fatalf("user restored from postgres must not buy twice, got %v", err)
	}
	if err := stock.Reserve(ctx, 3, 10, now); err != nil {
		t.Fatal(err)
	}
	if err := stock.Reserve(ctx, 3, 10, now); !errors.Is(err, domain.ErrAlreadyReserved) {
		t.Fatalf("want ErrAlreadyReserved, got %v", err)
	}
}

func TestInitIfAbsent_DoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	stock, _ := newTestStock(t)

	loaded, _ := stock.InitIfAbsent(ctx, 1, 10, time.Unix(0, 0), nil)
	if !loaded {
		t.Fatal("first init must load")
	}
	_ = stock.Reserve(ctx, 1, 7, time.Now())
	loaded, _ = stock.InitIfAbsent(ctx, 1, 10, time.Unix(0, 0), nil)
	if loaded {
		t.Fatal("second init must not overwrite live state")
	}
	if avail, _ := stock.Available(ctx, 1); avail != 9 {
		t.Fatalf("available %d, want 9", avail)
	}
}

// Повторная доставка order.expired не должна вернуть товар дважды — даже если
// пользователь между доставками успел снова забронировать.
func TestReleaseOrder_Idempotent(t *testing.T) {
	ctx := context.Background()
	stock, _ := newTestStock(t)
	_, _ = stock.InitIfAbsent(ctx, 1, 1, time.Unix(0, 0), nil)

	if err := stock.Reserve(ctx, 1, 7, time.Now()); err != nil {
		t.Fatal(err)
	}
	released, err := stock.ReleaseOrder(ctx, 1, 7, 100)
	if err != nil || !released {
		t.Fatalf("first release: released=%v err=%v", released, err)
	}

	// Пользователь бронирует снова (новый заказ 101).
	if err := stock.Reserve(ctx, 1, 7, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Дубликат старого события по заказу 100 приходит из Kafka ещё раз.
	released, err = stock.ReleaseOrder(ctx, 1, 7, 100)
	if err != nil || released {
		t.Fatalf("duplicate release must be a no-op: released=%v err=%v", released, err)
	}
	if avail, _ := stock.Available(ctx, 1); avail != 0 {
		t.Fatalf("available %d, want 0 (new reservation must stay)", avail)
	}
}

func TestRelease_Compensation(t *testing.T) {
	ctx := context.Background()
	stock, _ := newTestStock(t)
	_, _ = stock.InitIfAbsent(ctx, 1, 1, time.Unix(0, 0), nil)

	_ = stock.Reserve(ctx, 1, 7, time.Now())
	if err := stock.Release(ctx, 1, 7); err != nil {
		t.Fatal(err)
	}
	// Второй Release того же пользователя ничего не возвращает: его уже нет в множестве.
	_ = stock.Release(ctx, 1, 7)
	if avail, _ := stock.Available(ctx, 1); avail != 1 {
		t.Fatalf("available %d, want 1", avail)
	}
}

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()
	_, rdb := newTestStock(t)
	rl := NewRateLimiter(rdb, 3)
	now := time.Unix(1_700_000_000, 0)

	for i := 1; i <= 3; i++ {
		if ok, _ := rl.Allow(ctx, 1, now); !ok {
			t.Fatalf("request %d must be allowed", i)
		}
	}
	if ok, _ := rl.Allow(ctx, 1, now); ok {
		t.Fatal("4th request in the same second must be limited")
	}
	if ok, _ := rl.Allow(ctx, 2, now); !ok {
		t.Fatal("other user must not be affected")
	}
	if ok, _ := rl.Allow(ctx, 1, now.Add(time.Second)); !ok {
		t.Fatal("next window must allow again")
	}
}

func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	_, rdb := newTestStock(t)
	idem := NewIdempotency(rdb, time.Hour)

	stored, err := idem.Begin(ctx, 1, "k1")
	if err != nil || stored != nil {
		t.Fatalf("first Begin must acquire: %v %v", stored, err)
	}
	if _, err := idem.Begin(ctx, 1, "k1"); !errors.Is(err, ErrInProgress) {
		t.Fatalf("parallel duplicate must get ErrInProgress, got %v", err)
	}
	if err := idem.Complete(ctx, 1, "k1", StoredResponse{Status: 201, Body: []byte(`{"id":5}`)}); err != nil {
		t.Fatal(err)
	}
	stored, err = idem.Begin(ctx, 1, "k1")
	if err != nil || stored == nil || stored.Status != 201 || string(stored.Body) != `{"id":5}` {
		t.Fatalf("replay must return stored response: %+v %v", stored, err)
	}
	// Тот же ключ у другого пользователя — независимый запрос.
	if stored, err := idem.Begin(ctx, 2, "k1"); err != nil || stored != nil {
		t.Fatalf("keys must be scoped per user: %+v %v", stored, err)
	}
}
