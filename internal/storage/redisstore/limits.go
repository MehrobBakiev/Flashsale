package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------- rate limiter ----------

// rateScript — фиксированное окно в 1 секунду: INCR и EXPIRE атомарно.
// Если делать их двумя командами без Lua, при падении между ними ключ останется навсегда.
var rateScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return n
`)

type RateLimiter struct {
	rdb   *redis.Client
	limit int
}

func NewRateLimiter(rdb *redis.Client, perSecond int) *RateLimiter {
	return &RateLimiter{rdb: rdb, limit: perSecond}
}

func (l *RateLimiter) Allow(ctx context.Context, userID int64, now time.Time) (bool, error) {
	if l.limit <= 0 {
		return true, nil
	}
	key := fmt.Sprintf("rl:%d:%d", userID, now.Unix())
	n, err := rateScript.Run(ctx, l.rdb, []string{key}, 2).Int()
	if err != nil {
		return false, err
	}
	return n <= l.limit, nil
}

// ---------- idempotency ----------

// StoredResponse — то, что мы отдали клиенту в первый раз; на повтор отдаём то же самое.
type StoredResponse struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

var ErrInProgress = errors.New("idempotent request in progress")

const pendingMarker = "pending"

// lockTTL — сколько держим «замок», пока первый запрос обрабатывается.
// Если процесс упал посреди обработки, замок истечёт и клиент сможет повторить.
const lockTTL = 30 * time.Second

type Idempotency struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewIdempotency(rdb *redis.Client, ttl time.Duration) *Idempotency {
	return &Idempotency{rdb: rdb, ttl: ttl}
}

func idemKey(userID int64, key string) string {
	return fmt.Sprintf("idem:%d:%s", userID, key)
}

// Begin пытается захватить ключ. Возвращает:
//   - (nil, nil) — ключ наш, нужно выполнить запрос и вызвать Complete или Abort;
//   - (resp, nil) — запрос уже выполнялся, нужно вернуть сохранённый ответ;
//   - (nil, ErrInProgress) — такой же запрос сейчас выполняется параллельно.
func (i *Idempotency) Begin(ctx context.Context, userID int64, key string) (*StoredResponse, error) {
	k := idemKey(userID, key)
	ok, err := i.rdb.SetNX(ctx, k, pendingMarker, lockTTL).Result()
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	val, err := i.rdb.Get(ctx, k).Result()
	if errors.Is(err, redis.Nil) {
		// Ключ истёк между SETNX и GET — пробуем ещё раз.
		return i.Begin(ctx, userID, key)
	}
	if err != nil {
		return nil, err
	}
	if val == pendingMarker {
		return nil, ErrInProgress
	}
	var resp StoredResponse
	if err := json.Unmarshal([]byte(val), &resp); err != nil {
		return nil, fmt.Errorf("decode stored response: %w", err)
	}
	return &resp, nil
}

func (i *Idempotency) Complete(ctx context.Context, userID int64, key string, resp StoredResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return i.rdb.Set(ctx, idemKey(userID, key), data, i.ttl).Err()
}

// Abort снимает замок, если запрос завершился временной ошибкой — клиент сможет повторить.
func (i *Idempotency) Abort(ctx context.Context, userID int64, key string) error {
	return i.rdb.Del(ctx, idemKey(userID, key)).Err()
}
