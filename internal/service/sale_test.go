package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
)

type fakeRepo struct {
	Repository
	sale         domain.Sale
	activeUsers  []int64
	reserveErr   error
	reserveCalls int
}

func (f *fakeRepo) GetSale(_ context.Context, id int64) (domain.Sale, error) {
	if id != f.sale.ID {
		return domain.Sale{}, domain.ErrSaleNotFound
	}
	return f.sale, nil
}

func (f *fakeRepo) ActiveUsers(context.Context, int64) ([]int64, error) { return f.activeUsers, nil }

func (f *fakeRepo) CreateReservation(_ context.Context, saleID, userID int64, _ time.Duration) (domain.Order, error) {
	f.reserveCalls++
	if f.reserveErr != nil {
		return domain.Order{}, f.reserveErr
	}
	return domain.Order{ID: 1, SaleID: saleID, UserID: userID, Status: domain.StatusReserved}, nil
}

type fakeStock struct {
	loaded       bool
	reserveErr   error
	released     []int64
	initAvail    int
	initUsers    []int64
	reserveCalls int
}

func (f *fakeStock) Reserve(context.Context, int64, int64, time.Time) error {
	f.reserveCalls++
	if !f.loaded {
		return redisstore.ErrNotLoaded
	}
	return f.reserveErr
}

func (f *fakeStock) Release(_ context.Context, _, userID int64) error {
	f.released = append(f.released, userID)
	return nil
}

func (f *fakeStock) InitIfAbsent(_ context.Context, _ int64, avail int, _ time.Time, users []int64) (bool, error) {
	f.loaded, f.initAvail, f.initUsers = true, avail, users
	return true, nil
}

func (f *fakeStock) Available(context.Context, int64) (int, error) { return 0, nil }

func newSvc(repo *fakeRepo, stock *fakeStock) *SaleService {
	return NewSaleService(repo, stock, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestReserve_Success(t *testing.T) {
	repo := &fakeRepo{sale: domain.Sale{ID: 1, TotalStock: 10}}
	stock := &fakeStock{loaded: true}
	order, err := newSvc(repo, stock).Reserve(context.Background(), 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	if order.UserID != 7 || order.Status != domain.StatusReserved {
		t.Fatalf("unexpected order %+v", order)
	}
}

// Если Redis отказал (товар закончился), до базы запрос не доходит.
func TestReserve_RedisRejects_DoesNotHitDB(t *testing.T) {
	repo := &fakeRepo{sale: domain.Sale{ID: 1}}
	stock := &fakeStock{loaded: true, reserveErr: domain.ErrSoldOut}
	_, err := newSvc(repo, stock).Reserve(context.Background(), 1, 7)
	if !errors.Is(err, domain.ErrSoldOut) {
		t.Fatalf("want ErrSoldOut, got %v", err)
	}
	if repo.reserveCalls != 0 {
		t.Fatal("rejected request must not reach postgres")
	}
}

// Если запись в базу упала, единица в Redis должна вернуться (компенсация).
func TestReserve_DBFailure_Compensates(t *testing.T) {
	repo := &fakeRepo{sale: domain.Sale{ID: 1}, reserveErr: errors.New("connection reset")}
	stock := &fakeStock{loaded: true}
	_, err := newSvc(repo, stock).Reserve(context.Background(), 1, 7)
	if err == nil {
		t.Fatal("want error")
	}
	if len(stock.released) != 1 || stock.released[0] != 7 {
		t.Fatalf("stock must be released for user 7, got %v", stock.released)
	}
}

// Redis потерял данные — сервис восстанавливает их из Postgres и продолжает.
func TestReserve_RestoresStateFromPostgres(t *testing.T) {
	repo := &fakeRepo{sale: domain.Sale{ID: 1, TotalStock: 10, Reserved: 3}, activeUsers: []int64{1, 2, 3}}
	stock := &fakeStock{loaded: false}
	if _, err := newSvc(repo, stock).Reserve(context.Background(), 1, 7); err != nil {
		t.Fatal(err)
	}
	if stock.initAvail != 7 || len(stock.initUsers) != 3 {
		t.Fatalf("state restored incorrectly: avail=%d users=%v", stock.initAvail, stock.initUsers)
	}
	if stock.reserveCalls != 2 {
		t.Fatalf("reserve must be retried once after load, calls=%d", stock.reserveCalls)
	}
}

func TestReserve_UnknownSale(t *testing.T) {
	repo := &fakeRepo{sale: domain.Sale{ID: 1}}
	stock := &fakeStock{loaded: false}
	if _, err := newSvc(repo, stock).Reserve(context.Background(), 99, 7); !errors.Is(err, domain.ErrSaleNotFound) {
		t.Fatalf("want ErrSaleNotFound, got %v", err)
	}
}

func TestCreateSale_Validation(t *testing.T) {
	svc := newSvc(&fakeRepo{}, &fakeStock{})
	for _, tc := range []struct {
		name  string
		stock int
	}{{"", 10}, {"   ", 10}, {"ok", 0}, {"ok", -5}} {
		if _, err := svc.CreateSale(context.Background(), tc.name, tc.stock, time.Time{}); !errors.Is(err, domain.ErrInvalidInput) {
			t.Errorf("CreateSale(%q, %d): want ErrInvalidInput, got %v", tc.name, tc.stock, err)
		}
	}
}
