# SESSION HANDOFF — Faz 1 / Hafta 2

**Son güncelleme:** 2026-05-15
**Aktif milestone:** M3 — Consumer / Saga-Step-1 (inventory-service)

---

## Tamamlananlar

### M1 — Order-service + Outbox yazımı  ✅
- `orders`, `order_items`, `outbox_events`, `processed_events` tabloları (V1__init.sql, Flyway).
- `POST /orders` akışı: tek transaction içinde order + outbox satırı yazılıyor.
- Sınıflar: `OrderController`, `OrderServiceImpl`, `OrderPersister`, `OrderFactory`, `OutboxEvent`, repo'lar, DTO/wrapper, `GlobalExceptionHandler`.

### M2 — Outbox poller → Kafka publish  ✅
- `EventPublisher` (`@Scheduled fixedDelay=1000`, `@Transactional`).
- `selectTop100PublishedFalseEvent()` ile PENDING batch çekiliyor.
- **Fix:** `Map<String,Object>` payload Jackson `ObjectMapper.writeValueAsString()` ile JSON string'e çevriliyor (önceki LinkedHashMap serialization hatası giderildi).
- `KafkaTemplate.send(topic, aggregateId, json).get(5s)` → başarılıysa `status=PUBLISHED`, `published_at=now()`.
- Hata olursa `retry_count++`, `last_error` set, satır PENDING kalıyor (sonraki tickte tekrar denenir).
- Doğrulandı: Kafka 9092'ye event akıyor, DB durumu güncelleniyor.

---

## Şu an nerede duruyoruz

| Servis | M1 | M2 | M3 |
|---|---|---|---|
| order-service | ✅ | ✅ | — |
| inventory-service | scaffold + V1 schema + seed | — | **sıradaki** |

`inventory-service` mevcut hali:
- `InventoryServiceApplication.java` (boş, sadece `@SpringBootApplication`).
- `V1__init.sql`: `inventory_items` (3 seed product: 100, 50, 0), `stock_reservations` (`uq_reservations_saga` unique), `outbox_events`, `processed_events`.
- `application.yml` mevcut.

---

## M3 — Yapılacaklar (Consumer / Saga-Step-1)

**Hedef:** `order.events` topic'inden `OrderCreated` event'ini al → stok kontrol → ya `StockReserved` ya `StockRejected` event'ini outbox'a yaz.

### Adımlar
1. **Kafka config düzeltmesi (BLOKER):** `application.yml`'de `bootstrap-servers: localhost:9094` → `localhost:9092` (docker-compose 9092 expose ediyor).
2. **Bağımlılıklar:** `pom.xml`'e `spring-kafka`, `spring-data-jpa`, `postgresql`, `flyway`, `lombok` (order-service ile aynı seti hizala).
3. **Entity'ler:** `InventoryItem`, `StockReservation`, `OutboxEvent`, `ProcessedEvent`.
4. **Repo'lar:** `IInventoryItemRepository`, `IStockReservationRepository`, `IProcessedEventRepository`, `IOutboxEventRepository`.
5. **Consumer config:** `KafkaConsumerConfig` — `ConcurrentKafkaListenerContainerFactory<String, String>`, manual ack stratejisi (önce DB commit, sonra ack); `JsonDeserializer` yerine `StringDeserializer` (publisher zaten JSON string basıyor).
6. **Listener:** `OrderEventsListener`
   - `@KafkaListener(topics="order.events", groupId="inventory-service")`
   - Payload'ı `ObjectMapper` ile `OrderCreatedEvent` DTO'ya parse et.
   - **Idempotency Katman 1:** `processed_events` INSERT — UNIQUE patlarsa skip + ack.
7. **Service:** `StockReservationService.handleOrderCreated(event)`
   - `@Transactional`
   - Her line item için `inventory_items` row'unu `SELECT ... FOR UPDATE` (pessimistic lock) ile çek.
   - Tüm item'larda `available_quantity >= qty` ise:
     - `available_quantity -= qty`, `reserved_quantity += qty`.
     - `stock_reservations` INSERT (status=RESERVED, saga_instance_id event'ten).
     - `outbox_events` INSERT: `StockReserved` → topic `inventory.events`.
   - Aksi halde:
     - `outbox_events` INSERT: `StockRejected` (reason: hangi product yetersiz).
   - **Idempotency Katman 2:** `uq_reservations_saga` UNIQUE — aynı saga 2. kez gelirse INSERT patlar, transaction rollback.
8. **EventPublisher:** order-service'tekiyle aynı poller'ı kopyala (`inventory.events` topic'ine basacak).
9. **Test akışı:**
   - 9092 OK mü? `docker ps`.
   - `POST /orders` (product `11111...` → happy path) → kafka-ui'de `order.events`'te event, sonra `inventory.events`'te `StockReserved`.
   - `POST /orders` (product `33333...` stok=0) → `StockRejected`.
   - Aynı event 2. kez consume edilirse: ikinci sefer skip.

### İşbölümü
- **Berat:** her sınıf için pseudo-comment'li skeleton (business logic niyeti).
- **Claude:** tüm gerçek Spring Boot kodu, config, boilerplate, `OrderCreatedEvent` DTO, exception handler.

---

## Açık sorular / ileri ertelenmiş
- Saga-service henüz yok (orchestrator). M3'te choreography ile gidiyoruz — inventory direkt `StockReserved` basıyor. Saga-service M4-M5'te gelince orchestration'a geçiş ADR'ye yazılacak.
- Topic'leri önceden yaratan init script (`infra/kafka/`) — şu an auto-create ile çalışıyor; production-grade init Hafta 3'e bırakıldı.
