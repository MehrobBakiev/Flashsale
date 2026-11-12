package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/metrics"
)

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// errorResponse переводит доменные ошибки в HTTP-статус и машиночитаемый код.
// Внутренние ошибки наружу не раскрываются.
func errorResponse(err error) (int, errorBody) {
	var b errorBody
	status, code, msg := http.StatusInternalServerError, "internal", "internal error"

	switch {
	case errors.Is(err, errMissingUser):
		status, code, msg = http.StatusUnauthorized, "unauthorized", err.Error()
	case errors.Is(err, domain.ErrInvalidInput):
		status, code, msg = http.StatusBadRequest, "invalid_input", err.Error()
	case errors.Is(err, domain.ErrSaleNotFound):
		status, code, msg = http.StatusNotFound, "sale_not_found", err.Error()
	case errors.Is(err, domain.ErrOrderNotFound):
		status, code, msg = http.StatusNotFound, "order_not_found", err.Error()
	case errors.Is(err, domain.ErrSoldOut):
		status, code, msg = http.StatusConflict, "sold_out", err.Error()
	case errors.Is(err, domain.ErrAlreadyReserved):
		status, code, msg = http.StatusConflict, "already_reserved", err.Error()
	case errors.Is(err, domain.ErrSaleNotStarted):
		status, code, msg = http.StatusConflict, "sale_not_started", err.Error()
	case errors.Is(err, domain.ErrOrderNotPayable):
		status, code, msg = http.StatusConflict, "order_not_payable", err.Error()
	case errors.Is(err, domain.ErrRequestInProgress):
		status, code, msg = http.StatusConflict, "request_in_progress", err.Error()
	case errors.Is(err, domain.ErrRateLimited):
		status, code, msg = http.StatusTooManyRequests, "rate_limited", err.Error()
	}
	b.Error.Code, b.Error.Message = code, msg
	return status, b
}

func writeError(w http.ResponseWriter, logger *slog.Logger, err error) {
	status, body := errorResponse(err)
	if status >= 500 {
		logger.Error("request failed", "err", err)
	}
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, status, body)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// ---------- middleware ----------

type middleware func(http.Handler) http.Handler

func chain(h http.Handler, mws ...middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// observe пишет метрики и лог по каждому запросу. Лейбл route берётся из шаблона
// маршрута (r.Pattern), а не из URL — иначе каждый id создал бы новую серию в Prometheus.
func observe(logger *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start)
			metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
			metrics.HTTPDuration.WithLabelValues(route).Observe(elapsed.Seconds())
			if rec.status >= 500 {
				logger.Warn("http request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", elapsed)
			} else {
				logger.Debug("http request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "duration", elapsed)
			}
		})
	}
}

func recoverer(logger *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic", "recover", rec, "stack", string(debug.Stack()))
					writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
