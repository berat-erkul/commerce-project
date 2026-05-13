-- Hafta 2 — Saga + Outbox — payment-service initial schema
-- Sorumluluğu: Stripe entegrasyonu + idempotency + refunds (compensation)

-- ============================================================
-- PAYMENTS
-- Bir payment satırı = bir Stripe PaymentIntent. capture_method=manual ile (TCC):
--   - status=AUTHORIZED: PaymentIntent oluşturuldu, müşterinin kartına hold konuldu, para çekilmedi
--   - status=CAPTURED:   capture() çağrıldı, para gerçekten çekildi
--   - status=CANCELLED:  cancel() çağrıldı (TCC compensation — refund'a gerek yok)
--   - status=FAILED:     PaymentIntent oluşturma fail oldu
-- saga_instance_id UNIQUE = domain-level idempotency (Katman 2)
-- ============================================================
CREATE TABLE payments (
    id                          UUID PRIMARY KEY,
    order_id                    UUID         NOT NULL,
    saga_instance_id            UUID         NOT NULL,
    amount                      NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency                    CHAR(3)      NOT NULL DEFAULT 'USD',
    stripe_payment_intent_id    VARCHAR(128),       -- pi_xxx
    status                      VARCHAR(32)  NOT NULL,   -- AUTHORIZED, CAPTURED, FAILED, CANCELLED
    created_at                  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    captured_at                 TIMESTAMPTZ,
    -- Aynı saga için 2 payment satırı YAZILAMAZ — UNIQUE constraint.
    -- Saga retry ProcessPayment command'ını tekrar yollarsa, INSERT burada patlar.
    CONSTRAINT uq_payments_saga UNIQUE (saga_instance_id)
);

CREATE INDEX idx_payments_order  ON payments(order_id);
CREATE INDEX idx_payments_status ON payments(status);

-- ============================================================
-- REFUNDS — para iadesi kaydı (compensation hattı)
-- Bir payment'a en fazla 1 refund — UNIQUE constraint.
-- refund_id_external Stripe'ın `re_xxx` ID'si (Stripe Idempotency-Key zinciri).
-- Audit için ayrı tablo; payments tablosu immutable kalır.
-- ============================================================
CREATE TABLE refunds (
    id                  UUID PRIMARY KEY,
    payment_id          UUID NOT NULL REFERENCES payments(id),
    refund_id_external  VARCHAR(128),      -- re_xxx (Stripe)
    amount              NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    status              VARCHAR(32)  NOT NULL,   -- PENDING, COMPLETED, FAILED
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    -- Bir payment'a iki kez refund YAZILAMAZ — domain-level idempotency.
    CONSTRAINT uq_refunds_payment UNIQUE (payment_id)
);

-- ============================================================
-- IDEMPOTENCY KEYS (Stripe API çağrıları için)
-- Stripe'a yolladığımız her isteğin Idempotency-Key header'ı buraya kaydedilir.
-- Aynı key'le tekrar çağrılırsa Stripe orijinal response'u döner — yani
-- idempotency zinciri DB'de bitmez, third-party API'ye uzanır.
-- ============================================================
CREATE TABLE idempotency_keys (
    idempotency_key     VARCHAR(128) PRIMARY KEY,
    operation           VARCHAR(64)  NOT NULL,     -- 'STRIPE_CHARGE', 'STRIPE_REFUND', 'STRIPE_CAPTURE'
    response_payload    JSONB,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
-- OUTBOX (saga'ya event yollar: PaymentCompleted, PaymentFailed, PaymentRefunded)
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
