// Package service — бизнес-логика распродажи. Зависит только от интерфейсов,
// поэтому тестируется без реальных Postgres и Redis.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
)

type Repository interface {
	CreateSale(ctx context.Context, name string, totalStock int, startsAt time.Time) (domain.Sale, error)
	GetSale(ctx context.Context, id int64) (domain.Sale, error)
	ActiveUsers(ctx context.Context, saleID int64) ([]int64, error)
	ListSaleIDs(ctx context.Context) ([]int64, error)
	CreateReservation(ctx context.Context, saleID, userID int64, ttl time.Duration) (domain.Order, error)
	PayOrder(ctx context.Context, orderID, userID int64) (domain.Order, error)
	GetOrder(ctx context.Context, orderID, userID int64) (domain.Order, error)
}

type StockStore interface {
	Reserve(ctx context.Context, saleID, userID int64, now time.Time) error
	Release(ctx context.Context, saleID, userID int64) error
	InitIfAbsent(ctx context.Context, saleID int64, available int, startsAt time.Time, activeUsers []int64) (bool, error)
	Available(ctx context.Context, saleID int64) (int, error)
}

type SaleService struct {
	repo   Repository
	stock  StockStore
	ttl    time.Duration
	now    func() time.Time
	logger *slog.Logger
}

func NewSaleService(repo Repository, stock StockStore, reservationTTL time.Duration, logger *slog.Logger) *SaleService {
	return &SaleService{repo: repo, stock: stock, ttl: reservationTTL, now: time.Now, logger: logger}
}

type SaleView struct {
	domain.Sale
	// Available — остаток по данным Redis (то, что видит покупатель).
	Available int `json:"available"`
}

func (s *SaleService) CreateSale(ctx context.Context, name string, totalStock int, startsAt time.Time) (domain.Sale, error) {
	name = strings.TrimSpace(name)
	if name == "" || totalStock <= 0 {
		return domain.Sale{}, fmt.Errorf("%w: name is required and total_stock must be positive", domain.ErrInvalidInput)
	}
	if startsAt.IsZero() {
		startsAt = s.now()
	}
	sale, err := s.repo.CreateSale(ctx, name, totalStock, startsAt)
	if err != nil {
		return domain.Sale{}, err
	}
	if _, err := s.stock.InitIfAbsent(ctx, sale.ID, sale.TotalStock, sale.StartsAt, nil); err != nil {
		// Не фатально: при первом резерве состояние подтянется из базы (loadState).
		s.logger.Warn("init sale in redis failed", "sale_id", sale.ID, "err", err)
	}
	return sale, nil
}

func (s *SaleService) GetSale(ctx context.Context, id int64) (SaleView, error) {
	sale, err := s.repo.GetSale(ctx, id)
	if err != nil {
		return SaleView{}, err
	}
	avail, err := s.stock.Available(ctx, id)
	if err != nil || avail < 0 {
		avail = sale.TotalStock - sale.Reserved
	}
	return SaleView{Sale: sale, Available: avail}, nil
}

// Reserve — горячий путь.
//
// Redis отсекает подавляющее большинство запросов (товар закончился, повторная попытка)
// за одну атомарную Lua-операцию, не трогая базу. До PostgreSQL доходят только
// «победители» — их не больше, чем единиц товара. PostgreSQL при этом остаётся источником
// истины и сам гарантирует инварианты (CHECK reserved <= total_stock, уникальный индекс).
func (s *SaleService) Reserve(ctx context.Context, saleID, userID int64) (domain.Order, error) {
	start := time.Now()
	order, err := s.reserve(ctx, saleID, userID)
	metrics.ReserveDuration.Observe(time.Since(start).Seconds())
	metrics.Reservations.WithLabelValues(reserveResult(err)).Inc()
	return order, err
}

func (s *SaleService) reserve(ctx context.Context, saleID, userID int64) (domain.Order, error) {
	if saleID <= 0 || userID <= 0 {
		return domain.Order{}, domain.ErrInvalidInput
	}

	err := s.stock.Reserve(ctx, saleID, userID, s.now())
	if errors.Is(err, redisstore.ErrNotLoaded) {
		// Redis мог перезапуститься без данных — восстанавливаем состояние из базы.
		if err = s.loadState(ctx, saleID); err != nil {
			return domain.Order{}, err
		}
		err = s.stock.Reserve(ctx, saleID, userID, s.now())
	}
	if err != nil {
		return domain.Order{}, err
	}

	order, err := s.repo.CreateReservation(ctx, saleID, userID, s.ttl)
	if err != nil {
		// Компенсация: в Redis единица уже списана, а заказа нет — возвращаем её.
		// WithoutCancel: даже если клиент отключился, компенсацию нужно довести до конца.
		if rerr := s.stock.Release(context.WithoutCancel(ctx), saleID, userID); rerr != nil {
			s.logger.Error("compensation failed: redis stock may be lower than real",
				"sale_id", saleID, "user_id", userID, "err", rerr)
		}
		if errors.Is(err, domain.ErrSoldOut) || errors.Is(err, domain.ErrAlreadyReserved) {
			// Redis пропустил запрос, а база — нет: значит кэш разошёлся с источником истины.
			s.logger.Warn("redis and postgres disagree", "sale_id", saleID, "user_id", userID, "err", err)
		}
		return domain.Order{}, err
	}
	return order, nil
}

func (s *SaleService) Pay(ctx context.Context, orderID, userID int64) (domain.Order, error) {
	if orderID <= 0 || userID <= 0 {
		return domain.Order{}, domain.ErrInvalidInput
	}
	return s.repo.PayOrder(ctx, orderID, userID)
}

func (s *SaleService) GetOrder(ctx context.Context, orderID, userID int64) (domain.Order, error) {
	return s.repo.GetOrder(ctx, orderID, userID)
}

// WarmUp загружает в Redis все распродажи, которых там нет. Вызывается при старте.
func (s *SaleService) WarmUp(ctx context.Context) error {
	ids, err := s.repo.ListSaleIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.loadState(ctx, id); err != nil {
			return fmt.Errorf("warm up sale %d: %w", id, err)
		}
	}
	return nil
}

func (s *SaleService) loadState(ctx context.Context, saleID int64) error {
	sale, err := s.repo.GetSale(ctx, saleID)
	if err != nil {
		return err
	}
	users, err := s.repo.ActiveUsers(ctx, saleID)
	if err != nil {
		return err
	}
	loaded, err := s.stock.InitIfAbsent(ctx, saleID, sale.TotalStock-sale.Reserved, sale.StartsAt, users)
	if err != nil {
		return fmt.Errorf("init redis state: %w", err)
	}
	if loaded {
		s.logger.Info("sale state loaded into redis", "sale_id", saleID, "available", sale.TotalStock-sale.Reserved)
	}
	return nil
}

func reserveResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, domain.ErrSoldOut):
		return "sold_out"
	case errors.Is(err, domain.ErrAlreadyReserved):
		return "already_reserved"
	case errors.Is(err, domain.ErrSaleNotStarted):
		return "not_started"
	case errors.Is(err, domain.ErrSaleNotFound):
		return "not_found"
	default:
		return "error"
	}
}
