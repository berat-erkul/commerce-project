-- Claim-then-send outbox: IN_FLIGHT ara state için claimed_at.
-- Poller PENDING satırı FOR UPDATE SKIP LOCKED ile claim'ler, IN_FLIGHT yapar,
-- tx dışında Kafka'ya basar. claimed_at = stale reclaim (claim'leyip ölen poller).
ALTER TABLE outbox_events ADD COLUMN claimed_at TIMESTAMPTZ;

-- Reclaim taraması sadece IN_FLIGHT satırlara baksın.
CREATE INDEX idx_outbox_inflight ON outbox_events(claimed_at) WHERE status = 'IN_FLIGHT';
