//go:build integration

package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/migrations"
)

// Запуск: TEST_POSTGRES_DSN=postgres://... go test -tags integration ./internal/storage/postgres/
func newTestRepo(t *testing.T) *Repo {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx := context.Background()
	pool, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	return NewRepo(pool)
}

// Даже без Redis база сама не даёт продать больше, чем есть:
// 200 параллельных попыток на 10 единиц → ровно 10 заказов.
func TestCreateReservation_DatabaseGuaranteesNoOversell(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	sale, err := repo.CreateSale(ctx, "test", 10, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	var ok, soldOut atomic.Int64
	var wg sync.WaitGroup
	for u := int64(1); u <= 200; u++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.CreateReservation(ctx, sale.ID, u, time.Minute)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, domain.ErrSoldOut):
				soldOut.Add(1)
			default:
				t.Errorf("unexpected: %v", err)
			}
		}()
	}
	wg.Wait()

	got, _ := repo.GetSale(ctx, sale.ID)
	if ok.Load() != 10 || got.Reserved != 10 {
		t.Fatalf("ok=%d reserved=%d, want 10", ok.Load(), got.Reserved)
	}
}

func TestCreateReservation_OneActiveOrderPerUser(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	sale, _ := repo.CreateSale(ctx, "test", 10, time.Now())

	if _, err := repo.CreateReservation(ctx, sale.ID, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateReservation(ctx, sale.ID, 1, time.Minute); !errors.Is(err, domain.ErrAlreadyReserved) {
		t.Fatalf("want ErrAlreadyReserved, got %v", err)
	}
	// Неудачная попытка не должна «съесть» единицу: транзакция откатилась целиком.
	got, _ := repo.GetSale(ctx, sale.ID)
	if got.Reserved != 1 {
		t.Fatalf("reserved=%d, want 1", got.Reserved)
	}
}

func TestExpireAndPay(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	sale, _ := repo.CreateSale(ctx, "test", 10, time.Now())

	live, _ := repo.CreateReservation(ctx, sale.ID, 1, time.Minute)
	stale, _ := repo.CreateReservation(ctx, sale.ID, 2, time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	expired, err := repo.ExpireReservations(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 || expired[0].ID != stale.ID {
		t.Fatalf("expected only stale order to expire, got %+v", expired)
	}
	if _, err := repo.PayOrder(ctx, stale.ID, 2); !errors.Is(err, domain.ErrOrderNotPayable) {
		t.Fatalf("expired order must not be payable, got %v", err)
	}
	if _, err := repo.PayOrder(ctx, live.ID, 999); !errors.Is(err, domain.ErrOrderNotFound) {
		t.Fatalf("foreign order must be not found, got %v", err)
	}
	paid, err := repo.PayOrder(ctx, live.ID, 1)
	if err != nil || paid.Status != domain.StatusPaid {
		t.Fatalf("pay: %+v %v", paid, err)
	}

	got, _ := repo.GetSale(ctx, sale.ID)
	if got.Reserved != 1 {
		t.Fatalf("reserved=%d, want 1 (one expired, one paid)", got.Reserved)
	}
	// После истечения пользователь может попробовать снова.
	if _, err := repo.CreateReservation(ctx, sale.ID, 2, time.Minute); err != nil {
		t.Fatalf("user must be able to reserve again after expiry: %v", err)
	}
}

func TestOutbox_RollbackOnPublishFailure(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	sale, _ := repo.CreateSale(ctx, "test", 10, time.Now())
	_, _ = repo.CreateReservation(ctx, sale.ID, 1, time.Minute)
	_, _ = repo.CreateReservation(ctx, sale.ID, 2, time.Minute)

	_, err := repo.ProcessOutbox(ctx, 10, func(context.Context, []OutboxRecord) error {
		return errors.New("kafka unavailable")
	})
	if err == nil {
		t.Fatal("want error")
	}
	if n, _ := repo.OutboxBacklog(ctx); n != 2 {
		t.Fatalf("events must stay unpublished after failure, backlog=%d", n)
	}

	var seen []OutboxRecord
	n, err := repo.ProcessOutbox(ctx, 10, func(_ context.Context, recs []OutboxRecord) error {
		seen = recs
		return nil
	})
	if err != nil || n != 2 || seen[0].EventType != domain.EventOrderReserved {
		t.Fatalf("n=%d err=%v seen=%+v", n, err, seen)
	}
	if n, _ := repo.OutboxBacklog(ctx); n != 0 {
		t.Fatalf("backlog=%d, want 0", n)
	}
}

func TestSaveNotification_Idempotent(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()
	ev := domain.OrderEvent{EventID: 42, Type: domain.EventOrderPaid, OrderID: 1, UserID: 1}
	first, _ := repo.SaveNotification(ctx, ev, "hi")
	second, _ := repo.SaveNotification(ctx, ev, "hi")
	if !first || second {
		t.Fatalf("first=%v second=%v, want true/false", first, second)
	}
}
