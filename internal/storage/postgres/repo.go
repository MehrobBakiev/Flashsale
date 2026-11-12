package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MehrobBakiev/flashsale/internal/domain"
)

const uniqueViolation = "23505"

type Repo struct {
	pool *pgxpool.Pool
}

func NewRepo(pool *pgxpool.Pool) *Repo {
	return &Repo{pool: pool}
}

func (r *Repo) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ---------- sales ----------

func (r *Repo) CreateSale(ctx context.Context, name string, totalStock int, startsAt time.Time) (domain.Sale, error) {
	var s domain.Sale
	err := r.pool.QueryRow(ctx, `
		INSERT INTO sales (name, total_stock, starts_at)
		VALUES ($1, $2, $3)
		RETURNING id, name, total_stock, reserved, starts_at, created_at`,
		name, totalStock, startsAt,
	).Scan(&s.ID, &s.Name, &s.TotalStock, &s.Reserved, &s.StartsAt, &s.CreatedAt)
	if err != nil {
		return domain.Sale{}, fmt.Errorf("insert sale: %w", err)
	}
	return s, nil
}

func (r *Repo) GetSale(ctx context.Context, id int64) (domain.Sale, error) {
	var s domain.Sale
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, total_stock, reserved, starts_at, created_at
		FROM sales WHERE id = $1`, id,
	).Scan(&s.ID, &s.Name, &s.TotalStock, &s.Reserved, &s.StartsAt, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Sale{}, domain.ErrSaleNotFound
	}
	if err != nil {
		return domain.Sale{}, fmt.Errorf("select sale: %w", err)
	}
	return s, nil
}

// ActiveUsers возвращает пользователей с активными заказами в распродаже.
// Нужно для восстановления состояния Redis из базы (источника истины).
func (r *Repo) ActiveUsers(ctx context.Context, saleID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT user_id FROM orders
		WHERE sale_id = $1 AND status IN ('reserved', 'paid')`, saleID)
	if err != nil {
		return nil, fmt.Errorf("select active users: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ListSaleIDs нужен для прогрева Redis при старте.
func (r *Repo) ListSaleIDs(ctx context.Context) ([]int64, error) {
	rows, err := r.pool.Query(ctx, `SELECT id FROM sales ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("select sale ids: %w", err)
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

// ---------- orders ----------

// CreateReservation в одной транзакции:
//  1. атомарно увеличивает sales.reserved, если остаток ещё есть;
//  2. создаёт заказ (уникальный индекс не даёт второй активный заказ того же пользователя);
//  3. пишет событие order.reserved в outbox.
func (r *Repo) CreateReservation(ctx context.Context, saleID, userID int64, ttl time.Duration) (domain.Order, error) {
	var order domain.Order
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE sales SET reserved = reserved + 1
			WHERE id = $1 AND reserved < total_stock`, saleID)
		if err != nil {
			return fmt.Errorf("increment reserved: %w", err)
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM sales WHERE id = $1)`, saleID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return domain.ErrSaleNotFound
			}
			return domain.ErrSoldOut
		}

		err = tx.QueryRow(ctx, `
			INSERT INTO orders (sale_id, user_id, status, expires_at)
			VALUES ($1, $2, 'reserved', now() + make_interval(secs => $3))
			RETURNING id, sale_id, user_id, status, expires_at, created_at, updated_at`,
			saleID, userID, ttl.Seconds(),
		).Scan(&order.ID, &order.SaleID, &order.UserID, &order.Status, &order.ExpiresAt, &order.CreatedAt, &order.UpdatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
				return domain.ErrAlreadyReserved
			}
			return fmt.Errorf("insert order: %w", err)
		}

		return insertOutbox(ctx, tx, domain.EventOrderReserved, order)
	})
	if err != nil {
		return domain.Order{}, err
	}
	return order, nil
}

// PayOrder переводит заказ reserved → paid, только если бронь ещё не истекла.
// Гонку с фоновым expirer решает сам PostgreSQL: оба делают UPDATE одной строки,
// второй после снятия блокировки перепроверяет WHERE на новой версии строки
// (READ COMMITTED) и ничего не меняет.
func (r *Repo) PayOrder(ctx context.Context, orderID, userID int64) (domain.Order, error) {
	var order domain.Order
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE orders SET status = 'paid', updated_at = now()
			WHERE id = $1 AND user_id = $2 AND status = 'reserved' AND expires_at > now()
			RETURNING id, sale_id, user_id, status, expires_at, created_at, updated_at`,
			orderID, userID,
		).Scan(&order.ID, &order.SaleID, &order.UserID, &order.Status, &order.ExpiresAt, &order.CreatedAt, &order.UpdatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1 AND user_id = $2)`, orderID, userID,
			).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return domain.ErrOrderNotFound
			}
			return domain.ErrOrderNotPayable
		}
		if err != nil {
			return fmt.Errorf("update order: %w", err)
		}
		return insertOutbox(ctx, tx, domain.EventOrderPaid, order)
	})
	if err != nil {
		return domain.Order{}, err
	}
	return order, nil
}

func (r *Repo) GetOrder(ctx context.Context, orderID, userID int64) (domain.Order, error) {
	var o domain.Order
	err := r.pool.QueryRow(ctx, `
		SELECT id, sale_id, user_id, status, expires_at, created_at, updated_at
		FROM orders WHERE id = $1 AND user_id = $2`, orderID, userID,
	).Scan(&o.ID, &o.SaleID, &o.UserID, &o.Status, &o.ExpiresAt, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, domain.ErrOrderNotFound
	}
	if err != nil {
		return domain.Order{}, fmt.Errorf("select order: %w", err)
	}
	return o, nil
}

// ExpireReservations переводит в expired пачку просроченных броней, возвращает остаток
// в sales.reserved и пишет события order.expired — всё в одной транзакции.
// FOR UPDATE SKIP LOCKED позволяет запускать несколько воркеров без двойной обработки.
func (r *Repo) ExpireReservations(ctx context.Context, limit int) ([]domain.Order, error) {
	var expired []domain.Order
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH due AS (
				SELECT id FROM orders
				WHERE status = 'reserved' AND expires_at <= now()
				ORDER BY expires_at
				LIMIT $1
				FOR UPDATE SKIP LOCKED
			)
			UPDATE orders o SET status = 'expired', updated_at = now()
			FROM due WHERE o.id = due.id
			RETURNING o.id, o.sale_id, o.user_id, o.status, o.expires_at, o.created_at, o.updated_at`, limit)
		if err != nil {
			return fmt.Errorf("expire orders: %w", err)
		}
		expired, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Order, error) {
			var o domain.Order
			err := row.Scan(&o.ID, &o.SaleID, &o.UserID, &o.Status, &o.ExpiresAt, &o.CreatedAt, &o.UpdatedAt)
			return o, err
		})
		if err != nil {
			return err
		}

		perSale := make(map[int64]int)
		for _, o := range expired {
			perSale[o.SaleID]++
		}
		for saleID, n := range perSale {
			if _, err := tx.Exec(ctx, `UPDATE sales SET reserved = reserved - $2 WHERE id = $1`, saleID, n); err != nil {
				return fmt.Errorf("decrement reserved: %w", err)
			}
		}
		for _, o := range expired {
			if err := insertOutbox(ctx, tx, domain.EventOrderExpired, o); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return expired, nil
}

// ---------- outbox ----------

func insertOutbox(ctx context.Context, tx pgx.Tx, eventType string, o domain.Order) error {
	payload, err := json.Marshal(domain.OrderEvent{
		Type:       eventType,
		OrderID:    o.ID,
		SaleID:     o.SaleID,
		UserID:     o.UserID,
		Status:     o.Status,
		OccurredAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`,
		o.ID, eventType, payload)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

type OutboxRecord struct {
	ID          int64
	AggregateID int64
	EventType   string
	Payload     []byte
}

// ProcessOutbox блокирует пачку неопубликованных событий (SKIP LOCKED — безопасно для
// нескольких экземпляров relay), отдаёт их в publish и помечает опубликованными
// только если publish успешен. Если publish упал — транзакция откатывается,
// события останутся неопубликованными и уйдут в следующий раз (at-least-once).
func (r *Repo) ProcessOutbox(ctx context.Context, limit int, publish func(context.Context, []OutboxRecord) error) (int, error) {
	var n int
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id, aggregate_id, event_type, payload FROM outbox
			WHERE published_at IS NULL
			ORDER BY id
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return fmt.Errorf("select outbox: %w", err)
		}
		records, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutboxRecord, error) {
			var rec OutboxRecord
			err := row.Scan(&rec.ID, &rec.AggregateID, &rec.EventType, &rec.Payload)
			return rec, err
		})
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		if err := publish(ctx, records); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		ids := make([]int64, len(records))
		for i, rec := range records {
			ids[i] = rec.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return fmt.Errorf("mark published: %w", err)
		}
		n = len(records)
		return nil
	})
	return n, err
}

func (r *Repo) OutboxBacklog(ctx context.Context) (int64, error) {
	var n int64
	err := r.pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// ---------- notifications ----------

// SaveNotification возвращает false, если уведомление по этому событию уже было сохранено
// (повторная доставка из Kafka).
func (r *Repo) SaveNotification(ctx context.Context, ev domain.OrderEvent, message string) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO notifications (event_id, order_id, user_id, event_type, message)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		ev.EventID, ev.OrderID, ev.UserID, ev.Type, message)
	if err != nil {
		return false, fmt.Errorf("insert notification: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
