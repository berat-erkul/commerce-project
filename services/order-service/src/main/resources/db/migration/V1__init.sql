-- Hafta 2 — Saga + Outbox — order-service initial schema
-- Sorumluluğu: order CRUD + REST giriş noktası + OrderCreated event publish

-- Orders aggregate
CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     UUID         NOT NULL,
    status          VARCHAR(32)  NOT NULL,    -- PENDING, CONFIRMED, CANCELLED, COMPLETED
    total_amount    NUMERIC(12, 2) NOT NULL CHECK (total_amount > 0),
    currency        CHAR(3)      NOT NULL DEFAULT 'USD',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status   ON orders(status);

-- Order line items (child of order)
CREATE TABLE order_items (
    id           UUID PRIMARY KEY,
    order_id     UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id   UUID NOT NULL,
    quantity     INT  NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0)
);

CREATE INDEX idx_order_items_order ON order_items(order_id);

-- ============================================================
-- OUTBOX PATTERN
-- order-service'in transactional metoduna 2 INSERT yapılır:
--   1) orders / order_items
--   2) outbox_events
-- Tek transaction → ya ikisi de commit ya hiçbiri.
-- Worker (poller) bu tablodan PENDING'leri çekip Kafka'ya basar.
-- ============================================================
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(64) NOT NULL,    -- 'Order'
    aggregate_id    UUID        NOT NULL,
    event_type      VARCHAR(64) NOT NULL,    -- 'OrderCreated', 'OrderCompleted', ...
    topic           VARCHAR(128) NOT NULL,    -- target Kafka topic
    payload         JSONB       NOT NULL,
    headers         JSONB,
    status          VARCHAR(16) NOT NULL DEFAULT 'PENDING',  -- PENDING, PUBLISHED, FAILED
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT
);

-- Partial index: poller her döngüde sadece PENDING satırları tarasın.
-- 1M PUBLISHED row varsa bile tarama maliyeti PENDING sayısına bağlı kalır.
CREATE INDEX idx_outbox_pending ON outbox_events(created_at) WHERE status = 'PENDING';

-- ============================================================
-- CONSUMER-LEVEL IDEMPOTENCY (İki katman idempotency — Katman 1)
-- Kafka redelivery (consumer crash, offset commit edemeden restart, vb.)
-- aynı event_id'yi 2. kere getirirse, INSERT bu tabloya UNIQUE patlar.
-- Yani consumer ilk işlemde event_id'yi yazar, 2. işlemde SKIP eder.
-- ============================================================
CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,
    consumer_name   VARCHAR(64)  NOT NULL,   -- e.g., 'order-saga-listener'
    topic           VARCHAR(128) NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_consumer ON processed_events(consumer_name);
