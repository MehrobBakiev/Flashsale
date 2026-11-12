package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/MehrobBakiev/flashsale/internal/domain"
	"github.com/MehrobBakiev/flashsale/internal/service"
	"github.com/MehrobBakiev/flashsale/internal/storage/redisstore"
)

type fakeService struct {
	SaleService
	reserveErr error
	calls      int
}

func (f *fakeService) Reserve(_ context.Context, saleID, userID int64) (domain.Order, error) {
	f.calls++
	if f.reserveErr != nil {
		return domain.Order{}, f.reserveErr
	}
	return domain.Order{ID: int64(f.calls), SaleID: saleID, UserID: userID, Status: domain.StatusReserved}, nil
}

func (f *fakeService) GetSale(context.Context, int64) (service.SaleView, error) {
	return service.SaleView{}, domain.ErrSaleNotFound
}

func newTestHandler(t *testing.T, svc SaleService, rateLimit int) http.Handler {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewHandler(svc, redisstore.NewRateLimiter(rdb, rateLimit), redisstore.NewIdempotency(rdb, time.Hour), nil, logger).Routes()
}

func doReserve(h http.Handler, user, idemKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sales/1/reservations", nil)
	if user != "" {
		req.Header.Set("X-User-ID", user)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReserve_IdempotentReplay(t *testing.T) {
	svc := &fakeService{}
	h := newTestHandler(t, svc, 100)

	first := doReserve(h, "7", "abc")
	second := doReserve(h, "7", "abc")

	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("codes: %d %d", first.Code, second.Code)
	}
	if svc.calls != 1 {
		t.Fatalf("service must be called once, got %d", svc.calls)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("replayed response must be marked")
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("bodies differ:\n%s\n%s", first.Body, second.Body)
	}
}

func TestReserve_ErrorMapping(t *testing.T) {
	cases := []struct {
		err  error
		code int
		want string
	}{
		{domain.ErrSoldOut, http.StatusConflict, "sold_out"},
		{domain.ErrAlreadyReserved, http.StatusConflict, "already_reserved"},
		{domain.ErrSaleNotFound, http.StatusNotFound, "sale_not_found"},
		{context.DeadlineExceeded, http.StatusInternalServerError, "internal"},
	}
	for _, tc := range cases {
		h := newTestHandler(t, &fakeService{reserveErr: tc.err}, 100)
		rec := doReserve(h, "7", "")
		var body errorBody
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if rec.Code != tc.code || body.Error.Code != tc.want {
			t.Errorf("%v: got %d/%s, want %d/%s", tc.err, rec.Code, body.Error.Code, tc.code, tc.want)
		}
	}
}

// 5xx не сохраняется под ключом идемпотентности — клиент должен иметь возможность повторить.
func TestReserve_ServerErrorNotCached(t *testing.T) {
	svc := &fakeService{reserveErr: context.DeadlineExceeded}
	h := newTestHandler(t, svc, 100)
	_ = doReserve(h, "7", "retry-me")
	svc.reserveErr = nil
	rec := doReserve(h, "7", "retry-me")
	if rec.Code != http.StatusCreated || svc.calls != 2 {
		t.Fatalf("retry after 5xx must execute again: code=%d calls=%d", rec.Code, svc.calls)
	}
}

func TestReserve_RateLimited(t *testing.T) {
	h := newTestHandler(t, &fakeService{}, 2)
	codes := []int{doReserve(h, "7", "").Code, doReserve(h, "7", "").Code, doReserve(h, "7", "").Code}
	if codes[2] != http.StatusTooManyRequests {
		t.Fatalf("3rd request must be limited, codes=%v", codes)
	}
}

func TestReserve_RequiresUser(t *testing.T) {
	h := newTestHandler(t, &fakeService{}, 100)
	if rec := doReserve(h, "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", rec.Code)
	}
}
