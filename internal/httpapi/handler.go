// Package httpapi — REST API поверх сервиса распродаж.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/service"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
)

type SaleService interface {
	CreateSale(ctx context.Context, name string, totalStock int, startsAt time.Time) (domain.Sale, error)
	GetSale(ctx context.Context, id int64) (service.SaleView, error)
	Reserve(ctx context.Context, saleID, userID int64) (domain.Order, error)
	Pay(ctx context.Context, orderID, userID int64) (domain.Order, error)
	GetOrder(ctx context.Context, orderID, userID int64) (domain.Order, error)
}

type RateLimiter interface {
	Allow(ctx context.Context, userID int64, now time.Time) (bool, error)
}

type Idempotency interface {
	Begin(ctx context.Context, userID int64, key string) (*redisstore.StoredResponse, error)
	Complete(ctx context.Context, userID int64, key string, resp redisstore.StoredResponse) error
	Abort(ctx context.Context, userID int64, key string) error
}

// ReadinessCheck — функция проверки зависимости (Postgres, Redis) для /readyz.
type ReadinessCheck func(ctx context.Context) error

type Handler struct {
	svc     SaleService
	limiter RateLimiter
	idem    Idempotency
	checks  map[string]ReadinessCheck
	logger  *slog.Logger
}

func NewHandler(svc SaleService, limiter RateLimiter, idem Idempotency, checks map[string]ReadinessCheck, logger *slog.Logger) *Handler {
	return &Handler{svc: svc, limiter: limiter, idem: idem, checks: checks, logger: logger}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/sales", h.createSale)
	mux.HandleFunc("GET /api/v1/sales/{id}", h.getSale)
	mux.HandleFunc("POST /api/v1/sales/{id}/reservations", h.reserve)
	mux.HandleFunc("POST /api/v1/orders/{id}/pay", h.pay)
	mux.HandleFunc("GET /api/v1/orders/{id}", h.getOrder)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", h.ready)
	return chain(mux, recoverer(h.logger), observe(h.logger))
}

type createSaleRequest struct {
	Name       string    `json:"name"`
	TotalStock int       `json:"total_stock"`
	StartsAt   time.Time `json:"starts_at"`
}

func (h *Handler) createSale(w http.ResponseWriter, r *http.Request) {
	var req createSaleRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, h.logger, err)
		return
	}
	sale, err := h.svc.CreateSale(r.Context(), req.Name, req.TotalStock, req.StartsAt)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	writeJSON(w, http.StatusCreated, sale)
}

func (h *Handler) getSale(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	sale, err := h.svc.GetSale(r.Context(), id)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, sale)
}

// reserve: POST /api/v1/sales/{id}/reservations
// Заголовки: X-User-ID (в реальной системе — из токена после API Gateway),
// Idempotency-Key (опционально) — безопасный повтор запроса при сетевых сбоях.
func (h *Handler) reserve(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	saleID, err := pathID(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	userID, err := userIDFrom(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	allowed, err := h.limiter.Allow(ctx, userID, time.Now())
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	if !allowed {
		writeError(w, h.logger, domain.ErrRateLimited)
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" {
		stored, err := h.idem.Begin(ctx, userID, idemKey)
		switch {
		case errors.Is(err, redisstore.ErrInProgress):
			writeError(w, h.logger, domain.ErrRequestInProgress)
			return
		case err != nil:
			writeError(w, h.logger, err)
			return
		case stored != nil:
			w.Header().Set("Idempotent-Replayed", "true")
			writeRaw(w, stored.Status, stored.Body)
			return
		}
	}

	order, err := h.svc.Reserve(ctx, saleID, userID)

	status, body := http.StatusCreated, any(order)
	if err != nil {
		status, body = errorResponse(err)
		if status >= 500 {
			h.logger.Error("reserve failed", "sale_id", saleID, "user_id", userID, "err", err)
		}
	}

	// Сериализуем один раз: повторный ответ по Idempotency-Key будет побайтно тем же.
	raw, err := json.Marshal(body)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	if idemKey != "" {
		// Сохраняем только детерминированные ответы (успех или бизнес-ошибка).
		// Временные сбои (5xx) не сохраняем — клиент должен иметь возможность повторить.
		bg := context.WithoutCancel(ctx)
		if status < 500 {
			if cerr := h.idem.Complete(bg, userID, idemKey, redisstore.StoredResponse{Status: status, Body: raw}); cerr != nil {
				h.logger.Warn("save idempotent response failed", "err", cerr)
			}
		} else if aerr := h.idem.Abort(bg, userID, idemKey); aerr != nil {
			h.logger.Warn("release idempotency key failed", "err", aerr)
		}
	}
	writeRaw(w, status, raw)
}

func (h *Handler) pay(w http.ResponseWriter, r *http.Request) {
	orderID, err := pathID(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	userID, err := userIDFrom(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	order, err := h.svc.Pay(r.Context(), orderID, userID)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (h *Handler) getOrder(w http.ResponseWriter, r *http.Request) {
	orderID, err := pathID(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	userID, err := userIDFrom(r)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	order, err := h.svc.GetOrder(r.Context(), orderID, userID)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	result := make(map[string]string, len(h.checks))
	status := http.StatusOK
	for name, check := range h.checks {
		if err := check(ctx); err != nil {
			result[name] = err.Error()
			status = http.StatusServiceUnavailable
			continue
		}
		result[name] = "ok"
	}
	writeJSON(w, status, result)
}

// ---------- helpers ----------

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, domain.ErrInvalidInput
	}
	return id, nil
}

func userIDFrom(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.Header.Get("X-User-ID"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errMissingUser
	}
	return id, nil
}

var errMissingUser = errors.New("X-User-ID header is required")

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.Join(domain.ErrInvalidInput, err)
	}
	return nil
}
