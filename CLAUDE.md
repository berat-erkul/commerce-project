# commerce-lab — operating manual for Claude

Kurumsal mid-level mülakat hazırlığı için microservices showcase. Berat tek geliştirici, öğrenme projesi. Stack: Java 21 + Spring Boot 3.4 + Postgres 16 + Kafka KRaft + Redis + ES8.

## Read these BEFORE doing anything

1. **`SESSION-HANDOFF.md`** — current state, ⚡ NEXT block has next action with locked decisions. Living doc.
2. **`docs/adr/0001-*.md`** — saga orchestration decision (hybrid pattern, topic naming, why dedicated saga-service).
3. **`libs/common-events/{Order,Payment,Inventory}Events.java`** — wire contracts, **otoriter**. Plan ↔ library çelişirse library kazanır.
4. **`docs/SESSION-PROMPT.md`** — bootstrap prompt; verification + plan request before coding.

Roadmap (faz tablosu): `/Users/beraterkul/Documents/WORK/I'm on my way/ROADMAP.md`. Faz-kapı kuralı: faz atlama yok, geçiş için commerce-project entegrasyonu + sözlü Q&A şart. Şu an: **Faz 1 / Hafta 2 — Saga + Outbox.**

## Working agreement

- Berat = pseudo-comment'li skeleton (business logic niyeti). Claude = tüm gerçek Spring Boot kod + boilerplate + Sokratik anlatım. Berat "ya sen yaz" derse → tam yaz + anlat.
- **Factory/Persister split her saga akışı için zorunlu.** Factory = no DB, no @Transactional. Persister = @Transactional, sadece repo. Orchestrator = coordinator. Referans: `services/saga-service/.../service/SagaStartFactory.java` + `SagaStartPersister.java`.
- **Postgres aborted-tx kuralı:** `@Transactional` içinde SQL hatasını catch edip continue ETME. Idempotent INSERT için `ON CONFLICT DO NOTHING` native query (örn. `IProcessedEventRepository.insertIfAbsent`). UNIQUE race için exception @Transactional sınırından dışarı, üst katman yakalasın.
- **Berat'ın itirazlarına ciddiye al** — bu projedeki 2 mimari refactor (factory split + Postgres hardening) onun yakaladığı sorunlardan çıktı.
- **Her task bittiğinde:** (a) commit mesajı öner — kısa subject + 3-5 satır body, "is complete" tarzı; (b) `SESSION-HANDOFF.md`'in LIVING DOCUMENT bölümlerini güncelle (⚡ NEXT, 📍 STATUS, 🪤 KEY LEARNINGS, 🎯 SCOPE DECISIONS, 📂 FILE INVENTORY).
- TaskList'i kullan; #ID'ler handoff'ta refere ediliyor.

## Never

- 🚫 `entity/enums/` alt-paketini taşıma (Berat kasıtlı).
- 🚫 `SagaOrchestrator`'ı şişirme (coordinator kalmalı).
- 🚫 `@Transactional` içinde Kafka I/O ekleme. Mevcut `OutboxPublisher` Day 3'te async refactor edilecek (Task #12); yeni listener'larda aynı pattern'ı **tekrar** üretme.
- 🚫 Saf TCC modeline dönme. Common-events Seçenek B (`ProcessPayment`/`RefundPayment` + `PaymentCompleted`/`PaymentFailed`/`PaymentRefunded`).
- 🚫 Kafka portunu değiştirme (9092 sabit, fix uygulandı).
- 🚫 Yeni ADR oluşturma (ADR-002 Gün 3 sonunda toplu).
- 🚫 Cross-service polling. Cross-service state değişikliği = outbox + event.
- 🚫 Memory'de yazılı olmayan "yardımcı doc/note" dosyası açma. Tek doc kanalı: `SESSION-HANDOFF.md` + `docs/adr/`.

## Patterns reused across services (referans çek, yeniden derive ETME)

- 2-katman idempotency: K1 (`processed_events.event_id` PK) + K2 (servis-spesifik UNIQUE; saga için `(saga_type, order_id)`, payment için `saga_instance_id`, inventory için `saga_instance_id`).
- Header standardı: `saga-instance-id` + `event-type` her emission'da. Producer-side: `OutboxFactory` zorlar.
- Manual ack: `MANUAL_IMMEDIATE`, `ack.acknowledge()` only-after-success.
- State machine = DB kolonu otorite. Guard her advance/fail metodun ilk SQL'inden sonra.
- Concurrent saga mutation = `@Lock(PESSIMISTIC_WRITE)` + `findByIdForUpdate`.

## When stuck or context unclear

Tahmin etme, sor. Berat kendi kelimeleriyle açıklamayı tercih eder ("ya sen yaz" demediği sürece). Yeni karar gerekiyorsa scope decision listesine eklemeden önce onayını al, sonra handoff'a düş.
