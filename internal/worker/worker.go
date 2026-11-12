// Package worker — фоновые задачи: истечение броней и обработчики событий из Kafka.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/consumer"
	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
)

// ---------- expirer ----------

type ExpireStore interface {
	ExpireReservations(ctx context.Context, limit int) ([]domain.Order, error)
}

// Expirer периодически переводит просроченные брони в expired.
// Сам Redis он не трогает: возврат остатка делает потребитель события order.expired.
// Так база и outbox остаются единственным источником изменений, а Redis догоняет через Kafka.
type Expirer struct {
	store    ExpireStore
	interval time.Duration
	batch    int
	logger   *slog.Logger
}

func NewExpirer(store ExpireStore, interval time.Duration, logger *slog.Logger) *Expirer {
	return &Expirer{store: store, interval: interval, batch: 500, logger: logger}
}

func (e *Expirer) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for {
			expired, err := e.store.ExpireReservations(ctx, e.batch)
			if err != nil {
				if ctx.Err() == nil {
					e.logger.Error("expire reservations", "err", err)
				}
				break
			}
			if len(expired) > 0 {
				metrics.OrdersExpired.Add(float64(len(expired)))
				e.logger.Info("reservations expired", "count", len(expired))
			}
			if len(expired) < e.batch {
				break
			}
		}
	}
}

// ---------- stock releaser ----------

type StockReleaser interface {
	ReleaseOrder(ctx context.Context, saleID, userID, orderID int64) (bool, error)
}

// ReleaseStockHandler возвращает единицу товара в Redis по событию order.expired.
// Идемпотентен: маркер по order_id в Redis не даст вернуть единицу дважды.
func ReleaseStockHandler(stock StockReleaser, logger *slog.Logger) consumer.Handler {
	return func(ctx context.Context, ev domain.OrderEvent) error {
		if ev.Type != domain.EventOrderExpired {
			return nil
		}
		if ev.OrderID == 0 || ev.SaleID == 0 || ev.UserID == 0 {
			return fmt.Errorf("%w: incomplete event %+v", consumer.ErrPermanent, ev)
		}
		released, err := stock.ReleaseOrder(ctx, ev.SaleID, ev.UserID, ev.OrderID)
		if err != nil {
			return err
		}
		logger.Info("stock released", "order_id", ev.OrderID, "sale_id", ev.SaleID, "released", released)
		return nil
	}
}

// ---------- notifier ----------

type NotificationStore interface {
	SaveNotification(ctx context.Context, ev domain.OrderEvent, message string) (bool, error)
}

// NotifyHandler «отправляет» уведомление пользователю. В реальной системе здесь был бы
// email/push-провайдер; запись в таблицу с PRIMARY KEY (event_id) делает обработку
// идемпотентной — при повторной доставке второе уведомление не уйдёт.
func NotifyHandler(store NotificationStore, logger *slog.Logger) consumer.Handler {
	return func(ctx context.Context, ev domain.OrderEvent) error {
		var msg string
		switch ev.Type {
		case domain.EventOrderReserved:
			msg = fmt.Sprintf("Order #%d reserved. Please pay before the reservation expires.", ev.OrderID)
		case domain.EventOrderPaid:
			msg = fmt.Sprintf("Order #%d paid. Thank you!", ev.OrderID)
		case domain.EventOrderExpired:
			msg = fmt.Sprintf("Order #%d expired: payment was not received in time.", ev.OrderID)
		default:
			return nil
		}
		if ev.EventID == 0 {
			return fmt.Errorf("%w: event without id", consumer.ErrPermanent)
		}
		inserted, err := store.SaveNotification(ctx, ev, msg)
		if err != nil {
			return err
		}
		if inserted {
			logger.Debug("notification sent", "user_id", ev.UserID, "event", ev.Type, "order_id", ev.OrderID)
		}
		return nil
	}
}
