# SESSION HANDOFF — Faz 1 / Hafta 2

**Son güncelleme:** 2026-05-15
**Aktif milestone:** M3-extended — Orchestrator Saga (saga-service) + LIFO compensation + DLQ + exponential retry + MANUAL_INTERVENTION
**Referans:** `docs/adr/0001-saga-orchestration-via-dedicated-service.md` (ADR-001)

---

## Tamamlananlar

- **M1** — order-service + outbox yazımı ✅
- **M2** — outbox poller → Kafka publish ✅ (Map→JSON string fix)
- **Kafka port fix** — inventory-service ✅, saga-service ✅ (9094 → 9092)

---

## Saga adım zinciri (ADR-001 + Saf TCC)

**Forward:**
1. `PROCESS_PAYMENT` — Stripe `PaymentIntent.create(capture_method=manual)` + confirm → authorize/hold
2. `RESERVE_STOCK` — inventory pessimistic lock → reservation INSERT
3. `CAPTURE_PAYMENT` — Stripe `PaymentIntent.capture()` → para gerçekten alınır
4. `OrderCompleted` — saga COMPLETED, order.status=CONFIRMED

**LIFO compensation (her FORWARD/COMPLETED step için ters yönde tetiklenir):**
- `RESERVE_STOCK` → `ReleaseStock` (reservation RESERVED→RELEASED, available_qty geri yüklenir)
- `PROCESS_PAYMENT` → `CancelPaymentAuthorization` (hold bırakılır; capture olmadığı için refund değil)
- `CAPTURE_PAYMENT` step'i fail olursa kendisi compensate edilmez (henüz COMPLETED değil); önceki adımlar LIFO compensate edilir.

**Önemli:** `PROCESS_PAYMENT` daha forward'da fail olursa (authorize fail) saga state=`FAILED`, compensation gerekmez (henüz tamamlanmış forward step yok).

---

## State machine (9 state)

| State | Anlam |
|---|---|
| PENDING | Saga oluştu, ilk forward step'e geçmedi |
| AWAITING_PAYMENT | ProcessPayment basıldı, response beklenir |
| AWAITING_STOCK | ReserveStock basıldı, response beklenir |
| AWAITING_CAPTURE | CapturePayment basıldı, response beklenir |
| COMPLETED | Tüm forward step'ler OK, OrderCompleted basıldı |
| FAILED | Terminal, compensation gerekmedi |
| COMPENSATING | Geri-alma command'ları yayınlanıyor (LIFO) |
| CANCELLED | Terminal, compensation başarıyla tamam |
| MANUAL_INTERVENTION | Compensation 10 retry + DLQ sonrası fail; watcher artık dokunmaz |

`idx_saga_active` partial index sadece aktif state'leri tarar — MANUAL_INTERVENTION, COMPLETED, CANCELLED, FAILED hariç.

---

## Topic mimarisi (ADR-001)

- `<domain>.commands` — single-consumer, imperative (saga-service basar)
- `<domain>.events` — broadcast, past-tense (servisler basar)

Kullanılan topic'ler:
- `order.events` → OrderCreated, OrderCompleted, OrderCancelled
- `payment.commands` → ProcessPayment, CapturePayment, CancelPaymentAuthorization
- `payment.events` → PaymentAuthorized, PaymentCaptured, PaymentAuthorizationCancelled, PaymentFailed
- `inventory.commands` → ReserveStock, ReleaseStock
- `inventory.events` → StockReserved, StockReservationFailed, StockReleased
- `saga.commands.StartOrderSaga` — ileri ertelendi, şu an saga doğrudan `order.events`'i dinler

---

## 3 günlük plan

### Gün 1 — Forward path uçtan uca (payment STUB ile)

1. saga-service entity/repo/enum (`SagaInstance`, `SagaStepHistory`, `OutboxEvent`, `ProcessedEvent`; `SagaStatus`, `StepName`, `StepType`, `StepStatus`).
2. saga-service `KafkaConsumerConfig` (StringDeserializer, manual ack), `KafkaProducerConfig`.
3. saga-service `OrderEventsListener` — `order.events.OrderCreated` → K1 processed_events → SagaInstance INSERT (uq_saga_order = K2) → step_history(FORWARD/PROCESS_PAYMENT/STARTED) → outbox ProcessPayment → `payment.commands`. state=AWAITING_PAYMENT.
4. saga-service generic `OutboxPublisher` (order-service'tekinden uyarlanmış; topic outbox row'undan okunur).
5. saga-service `PaymentEventsListener` — `PaymentAuthorized` → step COMPLETED + step(FORWARD/RESERVE_STOCK/STARTED) → outbox ReserveStock. state=AWAITING_STOCK.
6. saga-service `InventoryEventsListener` — `StockReserved` → step COMPLETED + step(FORWARD/CAPTURE_PAYMENT/STARTED) → outbox CapturePayment. state=AWAITING_CAPTURE.
7. Gün 1 için STUB payment-service `CapturePayment` geldiğinde anında `PaymentCaptured` basacak (Gün 2'de gerçek olacak).
8. saga-service `PaymentCaptured` → step COMPLETED → outbox OrderCompleted (`order.events`). state=COMPLETED.
9. **inventory-service M3 (gerçek):** entity/repo/listener (`inventory.commands.ReserveStock`), pessimistic lock service, 2-katman idempotency, EventPublisher poller.
10. **payment-service STUB:** tek listener — `payment.commands` (ProcessPayment, CapturePayment, CancelPaymentAuthorization hepsi) anında karşılığını event olarak basar. Stripe entegrasyonu yok.
11. order-service `OrderCompleted` listener → order.status=CONFIRMED.

**Gün 1 kabul:** Happy path zinciri tamamen yeşil; kafka-ui'de tüm topic'lerde sırayla mesajlar görünür; `saga_instance.status=COMPLETED`; `order.status=CONFIRMED`.

### Gün 2 — Real Stripe TCC + LIFO compensation

1. payment-service entity/repo (`Payment`, `OutboxEvent`, `ProcessedEvent`), Stripe SDK config (test mode key).
2. `ProcessPayment` handler: `PaymentIntent.create(capture_method=manual)` + `.confirm()` → PaymentAuthorized / PaymentFailed.
3. `CapturePayment` handler: `PaymentIntent.capture()` → PaymentCaptured / PaymentCaptureFailed.
4. `CancelPaymentAuthorization` handler: `PaymentIntent.cancel()` → PaymentAuthorizationCancelled.
5. saga-service sad-path:
   - `PaymentFailed` → state FAILED.
   - `StockReservationFailed` → state COMPENSATING → LIFO sweep (step_history WHERE type=FORWARD AND status=COMPLETED ORDER BY started_at DESC) → her biri için compensation command outbox'a → `CancelPaymentAuthorization`.
6. inventory-service `ReleaseStock` handler: RESERVED→RELEASED tek-yönlü transition (idempotency K2).
7. saga-service compensation event'lerini topla; tüm bekleyenler COMPLETED olunca state=CANCELLED + OrderCancelled → order.status=CANCELLED.

**Gün 2 kabul:** 4 senaryo:
- Happy → COMPLETED ✅
- Stripe authorize fail → FAILED, hiç compensation çalışmadı ✅
- Stok yetersiz → COMPENSATING → CancelPaymentAuthorization → CANCELLED ✅
- Capture fail → COMPENSATING → ReleaseStock + CancelPaymentAuthorization (LIFO) → CANCELLED ✅

### Gün 3 — Dayanıklılık

1. **SagaWatcher** (@Scheduled poll-interval=5s): aktif partial index'i tara, `next_retry_at < NOW()` olanların compensation step'lerini yeniden outbox'a bas.
2. **BackoffCalculator:** `initial(1s) × 2^attempt`, cap=4h, ±25% jitter. attempt ≥ 10 → state=MANUAL_INTERVENTION.
3. **Kafka error stack:** her consumer'a `ErrorHandlingDeserializer` + `DefaultErrorHandler(FixedBackOff(1s,3))` + `DeadLetterPublishingRecoverer` → `<topic>.DLT`. Retryable / non-retryable ayrımı (`addNotRetryableExceptions(JsonProcessingException.class, ...)`).
4. **Correlation id:** `saga_instance_id` outbox.headers JSONB'ye → publisher Kafka header'a → consumer MDC.
5. **Layer 4 reconciliation:** ADR-002'ye "deferred" notu (Hafta 3).
6. **Smoke 6 senaryo:** happy, payment-fail, stock-fail, capture-fail, bozuk-JSON-DLT, OrderCreated-iki-kere-K1+K2.
7. **ADR-002:** capture model, saga.commands deferral, Layer 4 deferral.

---

## Kilitlenmiş scope kararları (2026-05-15)

- State machine: pure JPA + service (kütüphane yok)
- Compensation: full LIFO chain (stock release + payment authorization cancel + order CANCELLED)
- Stripe model: **Saf TCC** — `capture_method=manual`, ayrı `CAPTURE_PAYMENT` step
- Gün 1 payment-service: STUB; gerçek Stripe Gün 2
- Backoff: exponential + jitter, max 10 attempt, 4h ceiling → MANUAL_INTERVENTION
- saga.commands.StartOrderSaga: deferred — saga doğrudan order.events.OrderCreated dinler
- Reconciliation (Layer 4): deferred to Hafta 3

---

## İşbölümü

- **Berat:** her sınıf için pseudo-comment'li skeleton (iş niyeti, business logic)
- **Claude:** tüm gerçek Spring Boot kodu, config, boilerplate, DTO, exception handler, Kafka stack, scheduled jobs, Sokratik öğretim
