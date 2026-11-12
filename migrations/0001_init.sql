-- Распродажи. reserved — сколько единиц занято активными заказами (reserved + paid).
-- CHECK (reserved <= total_stock) — последняя линия защиты от оверселла:
-- даже если Redis ошибётся, база не даст продать больше, чем есть.
CREATE TABLE sales (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT        NOT NULL,
    total_stock INT         NOT NULL CHECK (total_stock > 0),
    reserved    INT         NOT NULL DEFAULT 0,
    starts_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sales_reserved_range CHECK (reserved >= 0 AND reserved <= total_stock)
);

CREATE TABLE orders (
    id         BIGSERIAL PRIMARY KEY,
    sale_id    BIGINT      NOT NULL REFERENCES sales (id),
    user_id    BIGINT      NOT NULL,
    status     TEXT        NOT NULL CHECK (status IN ('reserved', 'paid', 'expired')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Один активный заказ на пользователя в рамках распродажи.
-- Частичный индекс: после истечения брони пользователь может попробовать снова.
CREATE UNIQUE INDEX orders_one_active_per_user
    ON orders (sale_id, user_id)
    WHERE status IN ('reserved', 'paid');

-- Для фонового поиска просроченных броней.
CREATE INDEX orders_reserved_expires_at
    ON orders (expires_at)
    WHERE status = 'reserved';

-- Transactional outbox: событие пишется в той же транзакции, что и изменение заказа,
-- а отдельный relay публикует его в Kafka. Так событие не теряется и не появляется
-- без соответствующего изменения в базе.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id BIGINT      NOT NULL,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Журнал отправленных уведомлений. PRIMARY KEY по event_id делает потребителя
-- идемпотентным: повторная доставка того же события из Kafka ничего не дублирует.
CREATE TABLE notifications (
    event_id   BIGINT PRIMARY KEY,
    order_id   BIGINT      NOT NULL,
    user_id    BIGINT      NOT NULL,
    event_type TEXT        NOT NULL,
    message    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
