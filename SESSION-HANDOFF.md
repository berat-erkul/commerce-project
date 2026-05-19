# SESSION HANDOFF — commerce-lab orchestrator saga (M3-extended)

**Son güncelleme:** 2026-05-18
**Faz/Hafta:** Faz 1 / Hafta 2 — Gün 1 (Task #4 mid-way: boilerplate done, StockReservationService kaldı)
**Otoriter referanslar:** `docs/adr/0001-saga-orchestration-via-dedicated-service.md` + `libs/common-events/*.java`

---

## ⚡ NEXT — sıradaki ilk eylem

**Task:** #4 devam — inventory-service **boilerplate katmanı bitti** (UNCOMMITTED), `StockReservationService` (business logic) + `InventoryCommandsListener` kaldı.

### ❗ İLK İŞ: UNCOMMITTED diff'i Berat'a yeniden anlat
Son session sonunda Berat "yarın bana tekrar anlat" dedi. Yeni session'da Berat'a şu uncommitted dosyaları **flow + amaç + neden** ile anlat (oturmadı tam, tekrar açıklama istendi):

- `services/inventory-service/src/main/resources/db/migration/V2__reservation_uniqueness_per_product.sql` — UNIQUE constraint (saga_instance_id) → (saga_instance_id, product_id) — multi-item order desteği
- `entity/{InventoryItem, StockReservation, OutboxEvent, ProcessedEvent}.java` — JPA mapping (V1 + V2 schema'ya göre)
- `repo/{IInventoryItemRepository (@Lock PESSIMISTIC_WRITE findByProductIdForUpdate), IStockReservationRepository (findBySagaInstanceIdAndProductId K2), IOutboxEventRepository, IProcessedEventRepository (insertIfAbsent ON CONFLICT)}.java`
- `service/OutboxFactory.java` — saga-service kopyası, header standardı (`saga-instance-id`, `event-type`)
- `config/Kafka{Producer,Consumer}Config.java` — saga-service kopyası (acks=all, idempotent, MANUAL_IMMEDIATE)
- `worker/OutboxPublisher.java` — saga-service kopyası (@Scheduled polling)
- `InventoryServiceApplication.java` — `@EnableScheduling` eklendi

Berat anladıktan sonra: **commit at**, sonra StockReservationService'e geç.

### Geri kalan iş (Task #4):
| Dosya | Sorumluluğu |
|---|---|
| `inventory-service/service/StockReservationService.java` | pessimistic lock per productId + atomic decrement + reservation row + outbox event (TÜM ITEM'LAR atomik, biri yetersizse rollback + StockReservationFailed) |
| `inventory-service/listener/InventoryCommandsListener.java` | `inventory.commands` consume; header discriminator (ReserveStock / ReleaseStock-stub Gün 1) + manual ack |

**Berat'ın karar vermesi gereken 5 nokta (StockReservationService için):**
1. Tek `reservationId` event'te (item başına ayrı değil) — onay bekleniyor
2. Atomicity: hepsi-veya-hiçbiri (biri yetersizse rollback + Failed event) — onay bekleniyor
3. K1 sonra K2 sırası: önce processed_events check, sonra reservation duplicate check (varsa aynı reservationId reuse → idempotent re-emit StockReserved)
4. Stok kontrolü: `availableQuantity >= requestedQuantity` (reserved counter ayrı bookkeeping)
5. Stok düşürme: `available -= qty; reserved += qty` (endüstri standardı)

**Kilitli kararlar (V1):**
1. **Pessimistic lock per `productId`** — `findByProductIdForUpdate` JPQL @Lock(PESSIMISTIC_WRITE).
2. **Stock decrement atomic with reservation insert** — tek `@Transactional` içinde.
3. **Yetersiz stok = StockReservationFailed event** (exception değil).
4. **Idempotency 2-katmanlı:** K1 processed_events + K2 `stock_reservations UNIQUE(saga_instance_id, product_id)` (V2 sonrası).
5. **Header standardı zorunlu** — OutboxFactory bunu zaten halletti.

---

## ✅ TAMAMLANAN — Task #3 (Gün 1)

**Saga-service 3 listener + 5 persister + 2 factory yapısı oturdu.** Aşağıdaki dosyalar üretildi/güncellendi:

**Listener (3):** `OrderEventsListener`, `PaymentEventsListener`, `InventoryEventsListener`
**Persister (5):** `SagaStartPersister`, `SagaAdvancePersister`, `SagaFailPersister`, `SagaCompletePersister`, `SagaCompensatePersister`
**Factory (3):** `SagaStartFactory`, `SagaAdvanceFactory`, `OutboxFactory`
**Orchestrator:** 5 handler (start, onPaymentCompleted, onPaymentFailed, onStockReserved, onStockReservationFailed)

**State machine tamamen kablolu (Gün 1 itibarıyla):**
```
OrderCreated  → PENDING → AWAITING_PAYMENT  (ProcessPayment outbox)
PaymentCompleted → AWAITING_STOCK            (ReserveStock outbox, paymentId payload'a yazılır)
PaymentFailed  → FAILED                       (OrderCancelled outbox, compensation YOK)
StockReserved  → COMPLETED                    (OrderCompleted outbox)
StockReservationFailed → COMPENSATING         (RefundPayment outbox — compensation BAŞLANGICI)
```

**SagaCompensatePersister'da MANUAL_INTERVENTION dalı:** `paymentId` payload'da yoksa (AdvancePersister çalışmadan StockReservationFailed gelmiş — anomali) saga MANUAL_INTERVENTION'a alınır, operatör müdahalesi gerekir.

**SagaPayload eklemesi:** `paymentId` (UUID) — AdvancePersister PaymentCompleted'da yazıyor, CompensatePersister RefundPayment komutunda kullanıyor.

**Eksik (Gün 2):** RefundCompleted listener — COMPENSATING → CANCELLED + OrderCancelled. `PaymentRefundedListener` veya PaymentEventsListener'da yeni case.

---

## 🚫 DO NOT — bu session yapma

- **`entity/enums/` alt paketini `entity/`'ye geri taşıma.** Berat kasıtlı taşıdı linter sonrası, mevcut import'lar ona göre.
- **`SagaOrchestrator`'ı tekrar şişirme.** Coordinator kalmalı; iş factory'lerde + persister'larda.
- **`saveAndFlush` döndürme.** ON CONFLICT pattern'i ve dışa-propagate kuralı kilitli.
- **`@Transactional` içinde Kafka I/O ekleme.** OutboxPublisher'ın mevcut `@Transactional + .get(5s)` pattern'i Gün 3'te refactor edilecek (Task #12). Yeni listener'larda **yeniden** aynı pattern'ı yerleştirme.
- **`order-service`/`inventory-service`/`payment-service` Kafka portunu değiştirme** — 9092 sabit, fix uygulandı.
- **Saf TCC modeline geri dönme.** Common-events kütüphanesi Seçenek B'yi destekliyor; CapturePayment / CancelPaymentAuthorization event'leri YOK.
- **Yeni ADR oluşturma.** ADR-002 Gün 3 sonunda toplu yazılacak.

---

## 📍 STATUS — neredeyiz

```
order-service          M1+M2 ✅ ──── üretiyor: OrderCreated → order.events
                                     YAPILACAK (Task #6): OrderCompleted/Cancelled listener

saga-service           Task #1 ✅ JPA layer (entity, enum, repo, dto)
                       Task #2 ✅ Kafka config + OutboxPublisher (with headers)
                       Task #3 ✅ 3 listener + 5 persister + state machine kablolu

inventory-service      V1 schema ✅  V2 migration ✅ (UNIQUE per saga+product)
                       Entity (4) ✅  Repo (4) ✅  OutboxFactory ✅
                       Kafka{Producer,Consumer}Config ✅  OutboxPublisher ✅
                       @EnableScheduling ✅
                                  ↓ SİZ BURADAYSINIZ ↓ (UNCOMMITTED)
                       YAPILACAK: StockReservationService (business logic) +
                       InventoryCommandsListener (transport)

payment-service        Yok
                       YAPILACAK (Task #5 STUB Gün 1; Task #8 gerçek Stripe Gün 2)
```

**Tasks (TaskList ID'leri):** #1 ✅ #2 ✅ #3 ✅ #4–#13 pending. #6 = order-service Completed+Cancelled listener; #12 = DLQ + correlation + outbox publisher async refactor (önemli — IN_FLIGHT state burada eklenecek).

---

## 🧱 SAGA WIRE — mimari özet

**Hybrid (ADR-001):** orchestration sipariş yerleştirme için (saga-service), choreography post-completion fan-out için (notification/analytics, scope dışı).

**Forward chain (Seçenek B, common-events otoriter):**
```
PROCESS_PAYMENT ──► RESERVE_STOCK ──► OrderCompleted
   (authorize+capture)   (pessimistic lock)
```
**LIFO compensation:** `RESERVE_STOCK → ReleaseStock`, `PROCESS_PAYMENT → RefundPayment`.

**State (9):** PENDING, AWAITING_PAYMENT, AWAITING_STOCK, COMPLETED, FAILED, COMPENSATING, CANCELLED, MANUAL_INTERVENTION (+`AWAITING_CAPTURE` Saf TCC için kalmıştı, Seçenek B'de kullanılmıyor — şimdilik enum'da duruyor, ADR-002'de temizleyebiliriz).

**Topic'ler (her biri ayrı sorumluluk):**
- `order.events` — order-service üretir (OrderCreated); saga-service üretir (OrderCompleted, OrderCancelled).
- `payment.commands` — saga-service üretir; payment-service tek consumer.
- `payment.events` — payment-service üretir; saga-service tek consumer.
- `inventory.commands` — saga-service üretir; inventory-service tek consumer.
- `inventory.events` — inventory-service üretir; saga-service tek consumer.

**Header standardı (her saga emission):** `saga-instance-id` + `event-type`. payment/inventory emission'ları da aynı standardı uygulamalı (Task #4, #5'te dikkat).

**Idempotency 2-katman:**
- K1 (event seviyesi): `processed_events.event_id` PK + `consumer_name + topic` namespacing.
- K2 (domain): `saga_instances UNIQUE(saga_type, order_id)` + `payments.saga_instance_id UNIQUE` + `stock_reservations.saga_instance_id UNIQUE` (servis-spesifik).

---

## 🧠 KEY CODE ANCHORS — fresh Claude için referans noktaları

| Disiplin | Dosya | Anchor |
|---|---|---|
| ON CONFLICT idempotent INSERT | `services/saga-service/src/main/java/com/commercelab/sagaservice/repo/IProcessedEventRepository.java` | `insertIfAbsent` native query — Postgres bypass |
| Factory/Persister split | `services/saga-service/.../service/SagaStartFactory.java` (no DB) + `SagaStartPersister.java` (@Transactional, repo only) | Aynı pattern Advance/Fail/Compensation için tekrarlanacak |
| Coordinator + race translation | `services/saga-service/.../service/SagaOrchestrator.java` | `try { persister.persist(...) } catch (DataIntegrityViolationException e) { log "race lost" }` |
| Outbox+headers emission | `services/saga-service/.../service/OutboxFactory.java` `build(...)` | Standart header'lar burada zorlanır |
| Manual ack listener | `services/saga-service/.../listener/OrderEventsListener.java` | `Acknowledgment ack` + `ack.acknowledge()` only-after-success |
| @Scheduled outbox publisher | `services/saga-service/.../worker/OutboxPublisher.java` | `ProducerRecord` + JSONB headers → Kafka headers (Day 3 refactor edilecek) |
| Order outbox referans | `services/order-service/.../service/impl/OrderFactory.java` + `OrderPersister.java` | Berat'ın orijinal pattern'i; saga ona göre kuruldu |

---

## 🎯 SCOPE DECISIONS (kilitli)

| Karar | Değer | Sebep tek satır |
|---|---|---|
| State machine implementation | Pure JPA + service | Spring Statemachine = magic; öğrenme şeffaflığı |
| Stripe model | Seçenek B (auth+capture beraber) | common-events otoriter; CapturePayment event'i yok |
| Payment service Day 1 | STUB (instant success) | Gün 1 odak: zincir; gerçek Stripe Gün 2 |
| Backoff | exponential 1s × 2^n, ±25% jitter, cap 4h, max 10 attempt | Sonra MANUAL_INTERVENTION; ADR-001 ile uyumlu |
| Compensation Layer 4 (reconciliation) | Hafta 3'e ertelendi | ADR pragmatic deferral |
| Outbox publisher async refactor | Gün 3 Task #12 | Şu an synchronous, IN_FLIGHT state ile birlikte gelecek |
| `saga.commands.StartOrderSaga` topic | Ertelendi | Saga şu an `order.events.OrderCreated` direkt dinler |
| order-service publisher header support | Gün 3 cleanup | Şu an saga'da lenient JSON discriminator hack var |
| libs/outbox-starter | Hafta 4 (DDD/refactor) | 3 servis duplikasyonu kasıtlı, deneyim için |
| Multi-instance saga-service | Hafta 3 | `SELECT FOR UPDATE SKIP LOCKED` poller'a eklenince |

---

## 🪤 KEY LEARNINGS — bu session'da kazanılan disiplinler

### L1 — Postgres aborted-tx trap (Berat yakaladı)
SQL hata → tx 'E' state → sonraki SQL `current transaction is aborted` patlar. Spring `@Transactional` içinde catch-and-continue MySQL'de yürür, Postgres'te yürümez. **Çözümler:** (a) Idempotent INSERT için `ON CONFLICT DO NOTHING` native query — exception fırlatmaz. (b) UNIQUE race için exception @Transactional sınırından dışarı çıksın, üst katman yakalasın. (c) Aynı tx'te "denemek + başka SQL" gerekiyorsa `Propagation.NESTED` (SAVEPOINT) veya `REQUIRES_NEW`. **Memory:** `feedback_postgres_aborted_tx.md`.

### L2 — Factory/Persister split + revize kural
order-service'in OrderFactory/OrderPersister disiplini = "CPU iş tx dışında, DB iş minimal `@Transactional` içinde". Berat itiraz etti ("connection çok süre açık"), saga'ya uyarlandı. **Revize:** Factory sınıfı kendi içinde DB-free olsun yeterli. Çağrı yeri tx içinde olabilir (örn. mevcut state okunması zorunluysa). "Factory ASLA tx içinde çağrılmaz" aşırı katı.

### L3 — OutboxPublisher 3 sin (Berat'ın analizi)
Mevcut `@Transactional` + senkron `.get(5s)` 3 sıkıntı: (a) connection starvation (8 dk açık tx mümkün), (b) Kafka async motorunu boşa harcama, (c) batch ortasında rollback → publisher restart'ında 99 duplicate publish. **Çözüm Gün 3'te (Task #12):** `@Transactional` kalk, `.whenCompleteAsync(callback, dbExecutor)` (Kafka thread'i serbest), per-row mini-tx ile mark, **`IN_FLIGHT` ara state'i** (claim-then-send) re-poll storm önler. Bulk update YAGNI — metrik gösterene kadar per-row.

### L4 — Üç altın kural (Hafta 1-2 damıtması)
1. **DB state kolonu otorite.** Event ne derse desin. Guard her advance'in başında.
2. **Concurrent mutation = pessimistic lock** kısa-tx + hot row için. Optimistic = nadir çakışma + idempotent retry için.
3. **Cross-service state değişikliği = outbox + event.** Asla cross-service polling değil. Terminal notification (OrderCompleted/Cancelled) bunun zorunlu çıktısı — saga FAILED bile olsa order-service haber almalı.

---

## 📂 FILE INVENTORY — saga-service

```
services/saga-service/
├── pom.xml
├── src/main/resources/
│   ├── application.yml                      ← port 9092, outbox+watcher config
│   └── db/migration/V1__init.sql            ← saga_instances + step_history + outbox + processed_events
└── src/main/java/com/commercelab/sagaservice/
    ├── SagaServiceApplication.java          ← @EnableScheduling
    ├── config/
    │   ├── KafkaProducerConfig.java         ← String/String, idempotent, acks=all
    │   └── KafkaConsumerConfig.java         ← StringDeserializer, MANUAL_IMMEDIATE
    ├── dto/SagaPayload.java                 ← tipli JSONB POJO (OrderItemSnapshot inner)
    ├── entity/
    │   ├── SagaInstance.java                ← @Enumerated(STRING) + JSONB payload
    │   ├── SagaStepHistory.java
    │   ├── OutboxEvent.java                 ← payload + headers JSONB
    │   ├── ProcessedEvent.java
    │   └── enums/{SagaStatus, StepName, StepType, StepStatus}.java
    ├── repo/
    │   ├── ISagaInstanceRepository.java     ← findBySagaTypeAndOrderId, watcher query (NEW: findByIdForUpdate gelecek)
    │   ├── ISagaStepHistoryRepository.java  ← LIFO query hazır
    │   ├── IOutboxEventRepository.java      ← findPendingBatch(Pageable)
    │   └── IProcessedEventRepository.java   ← insertIfAbsent ON CONFLICT (native)
    ├── service/
    │   ├── OutboxFactory.java               ← typed record → OutboxEvent + standart headers
    │   ├── SagaStartFactory.java            ← no DB, SagaStartAggregate
    │   ├── SagaStartPersister.java          ← @Transactional all-or-nothing
    │   └── SagaOrchestrator.java            ← coordinator + DataIntegrityViolation catch
    ├── listener/
    │   └── OrderEventsListener.java         ← lenient JSON discriminator (Day 3 cleanup)
    └── worker/
        └── OutboxPublisher.java             ← @Scheduled (Day 3 async refactor)

DAY 1 EKLENECEKLER (Task #3 devamı):
    service/SagaAdvanceFactory.java
    service/SagaAdvancePersister.java
    service/SagaFailPersister.java
    listener/PaymentEventsListener.java
    listener/InventoryEventsListener.java
```

---

## 🔄 LIVING DOCUMENT DISCIPLINE

Her task tamamlandığında bu dosyayı **mutlaka** güncelle:
- ⚡ NEXT bölümünü bir sonraki task'a göre yenile.
- 📍 STATUS bölümündeki yeşil ✅ kayıtlarını ekle.
- Yeni öğrenilen disiplin varsa 🪤 KEY LEARNINGS bölümüne ekle.
- Karar değişti veya yeni karar kilitlendiyse 🎯 SCOPE DECISIONS güncellensin.
- Yeni dosya eklendi/silindiyse 📂 FILE INVENTORY güncellensin.

Bu doc memory + git history dışında **session-arası bağlamın tek kaynağıdır.** Eskirse fresh Claude kayıp olur.

---

## 🚀 BOOTSTRAP PROMPT — yeni session'a yapıştır

```
~/Desktop/commerce-project — commerce-lab orchestrator saga, Faz 1/Hafta 2, Gün 1.
Task #4 mid-way: inventory-service boilerplate UNCOMMITTED, business logic henüz yok.

Sırayla yap:
1. SESSION-HANDOFF.md'yi oku — özellikle ⚡ NEXT (uncommitted diff listesi orada).
2. `git status` + `git diff --stat` — uncommitted dosyaları kendi gözünle gör.
3. libs/common-events/InventoryEvents.java + OrderItem.java tara — ReserveStock kontratı.
4. saga-service/.../service/SagaCompensatePersister.java oku — compensation tarafının
   inventory'den ne beklediği orada (StockReservationFailed → RefundPayment).

Sonra bana ÖNCE şunu ver:
A) Uncommitted dosyaların DETAYLI WALKTHROUGH'u (Berat'a tekrar anlatılacak —
   son session sonunda "yarın tekrar anlat" dedi, oturmadı). Her dosya için:
   - Hangi katman (transport / persistence / business / config)
   - Ne işe yarıyor (1-2 cümle)
   - Bu sınıf olmasa ne olurdu (özellikle pessimistic lock, ON CONFLICT, outbox factory)
   - Flow diagramı: ReserveStock geldikten sonra zincir nasıl işleyecek

Berat anladığını söyleyince:
B) Commit mesajını öner (kısa subject + 3-5 satır body, handoff stili).
C) StockReservationService 5-nokta karar tablosunu Berat'a sun (handoff ⚡ NEXT
   bölümünde liste var); onay alınca pseudo-comment iste, sonra Spring Boot'a çevir.

Kurallar:
- Her task bitince commit mesajı öner.
- Her task bitince SESSION-HANDOFF.md LIVING DOCUMENT bölümlerini güncelle.
- 🚫 DO NOT listesindeki şeyleri yapma.
- Business logic Berat pseudo-comment yazar, sen Spring Boot'a çevirirsin
  [[feedback_code_handoff_style]].
- Berat'ın itirazlarını ciddiye al.
```

---

## ⏭️ İLERİDEKİ GÜNLER — özet

**Gün 1 kalanı:** #3 (devam) → #4 (inventory M3) → #5 (payment STUB) → #6 (order Completed/Cancelled listener) → #7 (happy path smoke).

**Gün 2:** #8 gerçek Stripe (Seçenek B) → #9 LIFO compensation (SagaCompensationFactory + Persister) → #10 inventory ReleaseStock + 3 senaryo smoke.

**Gün 3:** #11 SagaWatcher + BackoffCalculator → #12 DLQ stack + correlation MDC + **outbox publisher async refactor + IN_FLIGHT state** + order-service publisher header support → #13 6 senaryo smoke + ADR-002.

Detaylı plan task açıklamalarında (TaskList).
