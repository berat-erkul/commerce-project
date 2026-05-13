-- Hafta 2 — Saga + Outbox — saga-service initial schema
-- Sorumluluğu: orchestrator state machine + compensation + watcher

-- ============================================================
-- SAGA INSTANCES — orchestrator'ın state machine kaydı
--
-- 9 state (compensation derinlemesinden):
--   PENDING           — saga oluşturuldu, henüz iş başlamadı
--   IN_PROGRESS       — generic ara durum (refactor'a yer)
--   AWAITING_PAYMENT  — ProcessPayment yayınlandı, response bekleniyor
--   AWAITING_STOCK    — ReserveStock yayınlandı, response bekleniyor
--   COMPLETED         — happy path bitti, OrderCompleted yayınlandı
--   FAILED            — terminal: compensation gerekmez (örn. validation fail)
--   COMPENSATING      — sad path: geri alma command'ları yayınlanıyor
--   CANCELLED         — terminal: compensation başarıyla tamam
--   MANUAL_INTERVENTION — 9. state: compensation 10 retry + DLQ sonrası da fail,
--                          operatör müdahalesi şart. Watcher artık bu satıra dokunmaz.
--
-- saga_type + order_id UNIQUE = domain idempotency (aynı sipariş için 2 saga yok)
-- ============================================================
CREATE TABLE saga_instances (
    id              UUID PRIMARY KEY,
    saga_type       VARCHAR(64)  NOT NULL,    -- 'ORDER_PLACEMENT'
    order_id        UUID         NOT NULL,
    status          VARCHAR(32)  NOT NULL,
    current_step    VARCHAR(64),
    payload         JSONB        NOT NULL,    -- paymentId, reservationId, amount, items vs.
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    retry_count     INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_retry_at   TIMESTAMPTZ,
    CONSTRAINT uq_saga_order UNIQUE (saga_type, order_id)
);

-- ============================================================
-- PARTIAL INDEX — watcher şeffaflığı
-- Watcher sadece "aktif" sagaları tarar: COMPLETED, CANCELLED, FAILED,
-- MANUAL_INTERVENTION bu setin DIŞINDA (Compensation Katman 3).
-- Yani MANUAL_INTERVENTION saga'ya geçildiği an, watcher onu görmez,
-- otomatik retry tetiklenmez — sadece operatör command'ıyla çıkar.
-- ============================================================
CREATE INDEX idx_saga_active ON saga_instances(updated_at)
    WHERE status IN ('PENDING', 'IN_PROGRESS', 'AWAITING_PAYMENT', 'AWAITING_STOCK', 'COMPENSATING');

-- Retry scheduling için
CREATE INDEX idx_saga_next_retry ON saga_instances(next_retry_at)
    WHERE next_retry_at IS NOT NULL;

CREATE INDEX idx_saga_order ON saga_instances(order_id);

-- ============================================================
-- SAGA STEP HISTORY — LIFO compensation için altın değer
-- 5 adımlı saga 4. adımda fail olursa: bu tablodan COMPLETED step'leri
-- TERS sırayla okur, her birinin compensation command'ını yayınlar.
-- Forward step + compensation step, ikisi de buraya yazılır (step_type ile ayrım).
-- ============================================================
CREATE TABLE saga_step_history (
    id                  UUID PRIMARY KEY,
    saga_instance_id    UUID         NOT NULL REFERENCES saga_instances(id) ON DELETE CASCADE,
    step_name           VARCHAR(64)  NOT NULL,   -- 'PROCESS_PAYMENT', 'RESERVE_STOCK', ...
    step_type           VARCHAR(16)  NOT NULL,   -- FORWARD, COMPENSATION
    status              VARCHAR(32)  NOT NULL,   -- STARTED, COMPLETED, FAILED
    payload             JSONB,
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ
);

CREATE INDEX idx_saga_step_history_saga ON saga_step_history(saga_instance_id, started_at);

-- ============================================================
-- OUTBOX (saga command yayınlar: ProcessPayment, ReserveStock, RefundPayment, vs.)
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
-- saga consume eder: order.events, payment.events, inventory.events
CREATE TABLE processed_events (
    event_id        UUID PRIMARY KEY,
    consumer_name   VARCHAR(64)  NOT NULL,
    topic           VARCHAR(128) NOT NULL,
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processed_events_consumer ON processed_events(consumer_name);
