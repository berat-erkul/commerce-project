# SESSION HANDOFF — commerce-lab orchestrator saga (M3-extended)

**Son güncelleme:** 2026-05-16
**Faz/Hafta:** Faz 1 / Hafta 2 — Gün 1 mid-way
**Otoriter referanslar:** `docs/adr/0001-saga-orchestration-via-dedicated-service.md` + `libs/common-events/*.java`

---

## ⚡ NEXT — sıradaki ilk eylem

**Task:** #3 (in_progress) — Listener 2/3 = `PaymentEventsListener` zinciri.

**Yazılacaklar (kesin liste, isimler, paketler):**
| Dosya | Sorumluluğu | Type |
|---|---|---|
| `service/SagaAdvanceFactory.java` | `AdvanceAggregate` record + `buildAdvanceToReserveStock(SagaInstance, PaymentCompleted)` | NEW, no DB |
| `service/SagaAdvancePersister.java` | `persistAdvanceToReserveStock(eventId, PaymentCompleted)` | NEW, @Transactional |
| `service/SagaFailPersister.java` | `failOnPaymentFailed(eventId, PaymentFailed)` — emits OrderCancelled | NEW, @Transactional |
| `listener/PaymentEventsListener.java` | header-discriminated dispatcher → orchestrator | NEW |
| `service/SagaOrchestrator.java` | + `onPaymentCompleted` + `onPaymentFailed` metodları | EDIT |
| `repo/ISagaInstanceRepository.java` | + `findByIdForUpdate(UUID)` with `@Lock(PESSIMISTIC_WRITE)` | EDIT |
| `repo/ISagaStepHistoryRepository.java` | + lookup en son STARTED FORWARD step | EDIT |

**Kilitli kararlar (re-derive ETME, doğrudan uygula):**

1. **Pessimistic write lock** her advance/fail başlangıcında (`findByIdForUpdate`).
   *Neden:* Kafka rebalance + duplicate redelivery senaryosunda iki thread paralel advance yaparsa lost-update + double command emission olur. Pessimistic kuyrukta tek thread garantisi verir; optimistic kısa-tx + hot row + sık duplicate kombinasyonunda retry storm yaratır.

2. **State guard** her metodun ilk SQL'inden sonra: `if (saga.status != EXPECTED) return SKIPPED_INVALID_STATE`.
   *Neden:* Üç savunma katmanından üçüncüsü (K1 + lock + state guard). State kolonu otorite — event ne derse desin, kolonun dediği geçer. Kafka'nın at-least-once + saga'nın ileri itilmiş hali kombinasyonu için kritik.

3. **PaymentFailed → `OrderCancelled` outbox'a Gün 1'de yazılır** (`order.events`, eventType=OrderCancelled).
   *Neden:* Compensation **değil**, terminal notification. order-service saga-service'i poll edemez (anti-pattern). FAILED state için `OrderCancelled` basmasak order.status sonsuza kadar PENDING'de takılı kalır. Gün 2'de `SagaCompensationPersister` da CANCELLED state'inde aynı event'i basacak — bir saga ya FAILED ya CANCELLED, ikisi birden değil → duplicate riski yok.

4. **Factory `@Transactional` içinde çağrılabilir** (revize kural).
   *Neden:* `SagaAdvanceFactory.buildAdvanceToReserveStock` `SagaInstance.payload`'a ihtiyaç duyar; payload DB'den geliyor, factory'yi tx dışına çıkarmak imkansız. Disiplin daralt: **factory sınıfı kendi içinde DB-free olsun yeterli**, çağrı yeri tx içinde olabilir. Ufak Jackson convertValue (`OutboxFactory.build` içinde) ms cinsinden, kabul.

**`SagaAdvancePersister.persistAdvanceToReserveStock` akışı (kesin sıra):**
1. K1: `processedRepo.findById(eventId)` → varsa `SKIPPED_DUPLICATE_EVENT`.
2. `findByIdForUpdate(event.sagaInstanceId())` → null ise `SKIPPED_UNKNOWN_SAGA`.
3. State guard: `saga.status != AWAITING_PAYMENT` → `SKIPPED_INVALID_STATE`.
4. PROCESS_PAYMENT step satırını bul (`stepRepo.findFirstBy...StatusOrderByStartedAtDesc(saga.id, PROCESS_PAYMENT, FORWARD, STARTED)`), `status=COMPLETED`, `completedAt=now`.
5. `factory.buildAdvanceToReserveStock(saga, event)` → `AdvanceAggregate` (yeni step + ReserveStock outbox).
6. `saga.status=AWAITING_STOCK`, `currentStep=RESERVE_STOCK`, `updatedAt=now`.
7. `stepRepo.save(agg.newStep)` + `outboxRepo.save(agg.commandOutbox)`.
8. `processedRepo.insertIfAbsent(eventId, "saga-service-payment", "payment.events", now)`.

**`SagaFailPersister.failOnPaymentFailed` akışı:**
1-3. (yukarı ile aynı; consumerName="saga-service-payment", state guard `AWAITING_PAYMENT`)
4. Step satırı `status=FAILED, completedAt`.
5. `saga.status=FAILED, completedAt, lastError=event.reason()`.
6. **OrderCancelled outbox satırı** (`order.events`, payload = `OrderEvents.OrderCancelled` record).
7. `insertIfAbsent`.

**Listener discriminator:** `payment.events` topic'inde event-type Kafka header'ından oku (payment-service'i biz yazıyoruz, header standardını uyguluyoruz). Header yoksa skip+ack + warn log.

**Test edilebilirlik:** Bu task uçtan uca akış sağlamaz. PaymentEventsListener tek başına test için Task #5 (payment-service STUB) lazım. Unit-level: `SagaAdvancePersister` testcontainers Postgres ile race + state guard + ON CONFLICT senaryoları.

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
                       Task #3 🟡 OrderEventsListener path tamam
                                  ↓ SİZ BURADAYSINIZ ↓
                                  PaymentEventsListener + InventoryEventsListener

inventory-service      V1 schema + Application class only
                       YAPILACAK (Task #4): entity, repo, ReserveStock listener,
                       pessimistic stock service, EventPublisher poller

payment-service        Yok
                       YAPILACAK (Task #5 STUB Gün 1; Task #8 gerçek Stripe Gün 2)
```

**Tasks (TaskList ID'leri):** #1 ✅ #2 ✅ #3 🟡 #4–#13 pending. #6 = order-service Completed+Cancelled listener; #12 = DLQ + correlation + outbox publisher async refactor (önemli — IN_FLIGHT state burada eklenecek).

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

`docs/SESSION-PROMPT.md` dosyasında da kopyası var (bağımsız, handoff güncellenince orası da güncellensin). Aşağı kopyalıyorum kolay erişim için:

```
~/Desktop/commerce-project — commerce-lab orchestrator saga (M3-extended), Faz 1/Hafta 2.

Sırayla yap:
1. SESSION-HANDOFF.md'yi oku — özellikle ⚡ NEXT, 🚫 DO NOT, 🎯 SCOPE DECISIONS, 🪤 KEY LEARNINGS bölümleri.
2. docs/adr/0001-*.md ve libs/common-events/{Order,Payment,Inventory}Events.java tara — wire kontratları otoriter.
3. saga-service'in mevcut dosya ağacını gör (handoff 📂 FILE INVENTORY).
4. TaskList'i çek; #3 in_progress olduğunu doğrula.

Sonra bana TEK MESAJDA şunları ver (3 cevap, 1 plan):
A) Postgres aborted-tx trap'i 1 cümle + commerce-project'teki bypass kuralımız.
B) Common-events Saf TCC mi Seçenek B mi destekliyor; saga forward chain hangi 3 step?
C) PaymentFailed → OrderCancelled neden Gün 1'de basılır (compensation değil de)?
PLAN: PaymentEventsListener task'ı için somut 5 maddelik uygulama planı (dosya isimleri + sıralama). Handoff ⚡ NEXT'tekiyle tutarlı olsun.

Cevabın hatalıysa düzelt; planın handoff ile çelişiyorsa benim açıklamamı bekle. Sonra Task #3'ü #3 ID üzerinden in_progress tutarak devam et — Claude yazar + anlatır modu.

Kurallar:
- Her task bitince commit mesajı öner (handoff'taki stil — kısa subject + 3-5 satır body).
- Her task bitince SESSION-HANDOFF.md'nin LIVING DOCUMENT bölümlerini güncelle.
- 🚫 DO NOT listesindeki şeyleri yapma; karar değişikliği gerekirse benden onay al.
- Berat'ın itirazlarına ciddiye al — geçmişte 2 mimari refactor onun yakaladığı sorunlardan çıktı.
```

---

## ⏭️ İLERİDEKİ GÜNLER — özet

**Gün 1 kalanı:** #3 (devam) → #4 (inventory M3) → #5 (payment STUB) → #6 (order Completed/Cancelled listener) → #7 (happy path smoke).

**Gün 2:** #8 gerçek Stripe (Seçenek B) → #9 LIFO compensation (SagaCompensationFactory + Persister) → #10 inventory ReleaseStock + 3 senaryo smoke.

**Gün 3:** #11 SagaWatcher + BackoffCalculator → #12 DLQ stack + correlation MDC + **outbox publisher async refactor + IN_FLIGHT state** + order-service publisher header support → #13 6 senaryo smoke + ADR-002.

Detaylı plan task açıklamalarında (TaskList).
