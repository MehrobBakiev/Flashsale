// Package domain содержит сущности и ошибки предметной области, не зависящие от хранилищ.
package domain

import (
	"errors"
	"time"
)

type OrderStatus string

const (
	StatusReserved OrderStatus = "reserved"
	StatusPaid     OrderStatus = "paid"
	StatusExpired  OrderStatus = "expired"
)

type Sale struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	TotalStock int       `json:"total_stock"`
	Reserved   int       `json:"reserved"`
	StartsAt   time.Time `json:"starts_at"`
	CreatedAt  time.Time `json:"created_at"`
}

type Order struct {
	ID        int64       `json:"id"`
	SaleID    int64       `json:"sale_id"`
	UserID    int64       `json:"user_id"`
	Status    OrderStatus `json:"status"`
	ExpiresAt time.Time   `json:"expires_at"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Типы событий, которые уходят в Kafka через outbox.
const (
	EventOrderReserved = "order.reserved"
	EventOrderPaid     = "order.paid"
	EventOrderExpired  = "order.expired"
)

// OrderEvent — полезная нагрузка события в Kafka.
// EventID нужен потребителям для идемпотентной обработки (at-least-once доставка).
type OrderEvent struct {
	EventID    int64       `json:"event_id"`
	Type       string      `json:"type"`
	OrderID    int64       `json:"order_id"`
	SaleID     int64       `json:"sale_id"`
	UserID     int64       `json:"user_id"`
	Status     OrderStatus `json:"status"`
	OccurredAt time.Time   `json:"occurred_at"`
}

var (
	ErrSaleNotFound      = errors.New("sale not found")
	ErrSaleNotStarted    = errors.New("sale has not started yet")
	ErrSoldOut           = errors.New("sold out")
	ErrAlreadyReserved   = errors.New("user already has an active order in this sale")
	ErrOrderNotFound     = errors.New("order not found")
	ErrOrderNotPayable   = errors.New("order is not in reserved state or reservation expired")
	ErrRateLimited       = errors.New("too many requests")
	ErrRequestInProgress = errors.New("request with this idempotency key is in progress")
	ErrInvalidInput      = errors.New("invalid input")
)
