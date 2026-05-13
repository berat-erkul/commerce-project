-- Hafta 2 — Saga + Outbox — inventory-service initial schema
-- Sorumluluğu: stok rezervasyonu + compensation (release)

-- ============================================================
-- INVENTORY ITEMS — gerçek stok kaynağı
-- available_quantity = satışa hazır, reserved_quantity = rezerve (saga süreci)
-- ============================================================
CREATE TABLE inventory_items (
    product_id           UUID PRIMARY KEY,
    available_quantity   INT NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity    INT NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- STOCK RESERVATIONS — saga'nın rezerve ettiği stok kayıtları
-- saga_instance_id UNIQUE = domain-level idempotency (Katman 2)
-- ============================================================
CREATE TABLE stock_reservations (
    id                  UUID PRIMARY KEY,
    order_id            UUID         NOT NULL,
    saga_instance_id    UUID         NOT NULL,
    product_id          UUID         NOT NULL REFERENCES inventory_items(product_id),
    quantity            INT          NOT NULL CHECK (quantity > 0),
    status              VARCHAR(32)  NOT NULL,   -- RESERVED, RELEASED, CONFIRMED
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    released_at         TIMESTAMPTZ,
    -- Aynı saga için 2 reservation satırı YAZILAMAZ.
    CONSTRAINT uq_reservations_saga UNIQUE (saga_instance_id)
);

CREATE INDEX idx_reservations_order   ON stock_reservations(order_id);
CREATE INDEX idx_reservations_status  ON stock_reservations(status);
CREATE INDEX idx_reservations_product ON stock_reservations(product_id);

-- ============================================================
-- SEED DATA — test senaryoları için
-- Product C (33333...): stok=0 → bu ürüne sipariş gelince saga compensation
-- tetiklenir. Happy path + sad path ikisi de test edilebilir.
-- ============================================================
INSERT INTO inventory_items (product_id, available_quantity) VALUES
    ('11111111-1111-1111-1111-111111111111', 100),  -- bol stok
    ('22222222-2222-2222-2222-222222222222', 50),   -- bol stok
    ('33333333-3333-3333-3333-333333333333', 0);    -- STOK YOK — compensation testi

-- ============================================================
-- OUTBOX
-- ============================================================
CREATE TABLE outbox_events (
    id              UUID PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,
    aggregate_id    UUID         NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    payload         JSONB        NOT NULL,
    headers         JSONB,
    status          VARCHAR(16)  NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    retry_count     INT          NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX idx_outbox_pending ON outbox_events(created_at) WHERE status = 'PENDING';

-- Consumer-level idempotency (Katman 1)
CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,
    consumer_name   VARCHAR(64)  NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_consumer ON processed_events(consumer_name);
