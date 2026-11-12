// Package redisstore — горячий путь в Redis: остатки, лимиты запросов, идемпотентность.
package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/MehrobBakiev/flashsale/internal/domain"
)

// Ключи распродажи используют hash tag {sale:ID}: в Redis Cluster все ключи одной
// распродажи попадают в один слот, иначе Lua-скрипт с несколькими ключами не выполнится.
func infoKey(saleID int64) string  { return fmt.Sprintf("{sale:%d}:info", saleID) }
func usersKey(saleID int64) string { return fmt.Sprintf("{sale:%d}:users", saleID) }
func releasedKey(saleID, orderID int64) string {
	return fmt.Sprintf("{sale:%d}:released:%d", saleID, orderID)
}

// reserveScript атомарно (Redis выполняет Lua целиком, без вклинивания других команд):
// проверяет старт распродажи, повторную покупку и остаток, затем резервирует единицу.
//
//	 1 — зарезервировано
//	 0 — товар закончился
//	-1 — у пользователя уже есть бронь
//	-2 — распродажа ещё не началась
//	-3 — состояние распродажи не загружено в Redis
var reserveScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -3
end
local starts_at = tonumber(redis.call('HGET', KEYS[1], 'starts_at'))
if tonumber(ARGV[2]) < starts_at then
  return -2
end
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 1 then
  return -1
end
local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))
if stock <= 0 then
  return 0
end
redis.call('HINCRBY', KEYS[1], 'stock', -1)
redis.call('SADD', KEYS[2], ARGV[1])
return 1
`)

// releaseScript возвращает единицу товара. Если передан третий ключ (маркер по order_id),
// скрипт идемпотентен: повторная доставка того же события order.expired ничего не сделает.
// Без маркера (компенсация в рамках того же запроса) защитой служит SREM:
// возвращаем остаток, только если пользователь действительно был в множестве.
var releaseScript = redis.NewScript(`
if #KEYS == 3 then
  if redis.call('SET', KEYS[3], '1', 'NX', 'EX', ARGV[2]) == false then
    return 0
  end
end
if redis.call('SREM', KEYS[2], ARGV[1]) == 1 then
  redis.call('HINCRBY', KEYS[1], 'stock', 1)
  return 1
end
return 0
`)

// initScript загружает состояние распродажи, только если его ещё нет.
// Это защищает от гонки нескольких экземпляров, одновременно прогревающих кэш.
var initScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], 'stock', ARGV[1], 'starts_at', ARGV[2])
redis.call('DEL', KEYS[2])
for i = 3, #ARGV do
  redis.call('SADD', KEYS[2], ARGV[i])
end
return 1
`)

var ErrNotLoaded = errors.New("sale state is not loaded into redis")

// releasedMarkerTTL — сколько помнить, что бронь уже вернули. Должно быть больше,
// чем максимальная задержка повторной доставки сообщения из Kafka.
const releasedMarkerTTL = 7 * 24 * time.Hour

type Stock struct {
	rdb *redis.Client
}

func NewStock(rdb *redis.Client) *Stock {
	return &Stock{rdb: rdb}
}

func (s *Stock) Reserve(ctx context.Context, saleID, userID int64, now time.Time) error {
	res, err := reserveScript.Run(ctx, s.rdb,
		[]string{infoKey(saleID), usersKey(saleID)},
		userID, now.Unix(),
	).Int()
	if err != nil {
		return fmt.Errorf("reserve script: %w", err)
	}
	switch res {
	case 1:
		return nil
	case 0:
		return domain.ErrSoldOut
	case -1:
		return domain.ErrAlreadyReserved
	case -2:
		return domain.ErrSaleNotStarted
	case -3:
		return ErrNotLoaded
	default:
		return fmt.Errorf("unexpected reserve result %d", res)
	}
}

// Release — компенсация, если после успешного резерва в Redis не удалось записать заказ в БД.
func (s *Stock) Release(ctx context.Context, saleID, userID int64) error {
	return releaseScript.Run(ctx, s.rdb,
		[]string{infoKey(saleID), usersKey(saleID)},
		userID,
	).Err()
}

// ReleaseOrder возвращает единицу по истёкшему заказу. Идемпотентно по orderID.
func (s *Stock) ReleaseOrder(ctx context.Context, saleID, userID, orderID int64) (bool, error) {
	res, err := releaseScript.Run(ctx, s.rdb,
		[]string{infoKey(saleID), usersKey(saleID), releasedKey(saleID, orderID)},
		userID, int(releasedMarkerTTL.Seconds()),
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// InitIfAbsent загружает остаток и список покупателей, если ключа ещё нет.
func (s *Stock) InitIfAbsent(ctx context.Context, saleID int64, available int, startsAt time.Time, activeUsers []int64) (bool, error) {
	args := make([]any, 0, 2+len(activeUsers))
	args = append(args, available, startsAt.Unix())
	for _, u := range activeUsers {
		args = append(args, u)
	}
	res, err := initScript.Run(ctx, s.rdb, []string{infoKey(saleID), usersKey(saleID)}, args...).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// Available — текущий остаток по данным Redis (-1, если состояние не загружено).
func (s *Stock) Available(ctx context.Context, saleID int64) (int, error) {
	v, err := s.rdb.HGet(ctx, infoKey(saleID), "stock").Result()
	if errors.Is(err, redis.Nil) {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(v)
}

func (s *Stock) Ping(ctx context.Context) error {
	return s.rdb.Ping(ctx).Err()
}
