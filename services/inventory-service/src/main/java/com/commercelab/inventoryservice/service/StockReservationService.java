package com.commercelab.inventoryservice.service;

import com.commercelab.events.InventoryEvents;
import com.commercelab.events.OrderItem;
import com.commercelab.inventoryservice.entity.InventoryItem;
import com.commercelab.inventoryservice.entity.OutboxEvent;
import com.commercelab.inventoryservice.entity.StockReservation;
import com.commercelab.inventoryservice.exception.InsufficientStockException;
import com.commercelab.inventoryservice.repo.IInventoryItemRepository;
import com.commercelab.inventoryservice.repo.IOutboxEventRepository;
import com.commercelab.inventoryservice.repo.IProcessedEventRepository;
import com.commercelab.inventoryservice.repo.IStockReservationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * ReserveStock komutunu işleyen business katmanı.
 * <ul>
 *   <li>Atomicity: hepsi-veya-hiçbiri (item'ların biri yetersizse rollback + Failed event).</li>
 *   <li>Idempotency K1: processed_events ON CONFLICT DO NOTHING (Postgres aborted-tx trap'ten kaçınma).</li>
 *   <li>Idempotency K2: stock_reservations UNIQUE(saga_instance_id, product_id) — defansif.</li>
 *   <li>Concurrency: per-product pessimistic write lock; deadlock önlemek için item'lar productId sıralı kilitlenir.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StockReservationService {

    static final String CONSUMER_NAME = "inventory-service-commands";
    static final String TOPIC_INVENTORY_COMMANDS = "inventory.commands";
    static final String TOPIC_INVENTORY_EVENTS = "inventory.events";

    private final IInventoryItemRepository invRepo;
    private final IStockReservationRepository reservationRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    public enum Result { RESERVED, SKIPPED_DUPLICATE, SKIPPED_ALL_ALREADY_RESERVED }

    public enum ReleaseResult { RELEASED, SKIPPED_DUPLICATE, SKIPPED_NOTHING_TO_RELEASE }

    /**
     * Reserve flow — tek tx. Yetersiz stokta InsufficientStockException → tx rollback;
     * listener ayrı tx'te emitFailed çağırır.
     */
    @Transactional
    public Result reserve(InventoryEvents.ReserveStock cmd) {
        OffsetDateTime now = OffsetDateTime.now();

        // K1 — processed_events guard. 0 = zaten işlenmiş, sessizce çık.
        int inserted = processedRepo.insertIfAbsent(
                cmd.commandId(), CONSUMER_NAME, TOPIC_INVENTORY_COMMANDS, now);
        if (inserted == 0) {
            log.info("K1 hit: commandId={} already processed, skipping", cmd.commandId());
            return Result.SKIPPED_DUPLICATE;
        }

        // Deadlock önlemek için productId sıralı kilitleme (iki paralel saga aynı çift ürünü
        // ters sırada lock'larsa Postgres deadlock detector birini öldürür — sıralı erişim
        // bunu eler).
        List<OrderItem> sortedItems = cmd.items().stream()
                .sorted(Comparator.comparing(OrderItem::productId))
                .toList();

        int reservedItems = 0;

        for (OrderItem item : sortedItems) {
            // K2 — saga + product için reservation zaten var mı?
            Optional<StockReservation> existing = reservationRepo.findBySagaInstanceIdAndProductId(
                    cmd.sagaInstanceId(), item.productId());
            if (existing.isPresent()) {
                log.info("K2 hit: saga={} product={} already reserved, skipping item",
                        cmd.sagaInstanceId(), item.productId());
                continue;
            }

            InventoryItem inv = invRepo.findByProductIdForUpdate(item.productId())
                    .orElseThrow(() -> new InsufficientStockException(
                            "Unknown product: " + item.productId()));

            if (inv.getAvailableQuantity() < item.quantity()) {
                throw new InsufficientStockException(
                        "Insufficient stock for product %s: available=%d, requested=%d".formatted(
                                item.productId(), inv.getAvailableQuantity(), item.quantity()));
            }

            inv.setAvailableQuantity(inv.getAvailableQuantity() - item.quantity());
            inv.setReservedQuantity(inv.getReservedQuantity() + item.quantity());
            inv.setUpdatedAt(now);

            StockReservation row = StockReservation.builder()
                    .id(UUID.randomUUID())
                    .orderId(cmd.orderId())
                    .sagaInstanceId(cmd.sagaInstanceId())
                    .productId(item.productId())
                    .quantity(item.quantity())
                    .status("RESERVED")
                    .createdAt(now)
                    .build();
            reservationRepo.save(row);
            reservedItems++;
        }

        // Tüm item'lar K2 hit → bu K1 olmadan K2 hit anomalisi (processed_events temizlenmiş ya da
        // restore sonrası). Karar #3 gereği re-emit yok; sessizce ack.
        if (reservedItems == 0) {
            log.warn("All items already reserved for saga={}, no StockReserved emitted",
                    cmd.sagaInstanceId());
            return Result.SKIPPED_ALL_ALREADY_RESERVED;
        }

        UUID reservationId = UUID.randomUUID();  // saga-level audit id; release sagaInstanceId üzerinden
        InventoryEvents.StockReserved event = new InventoryEvents.StockReserved(
                UUID.randomUUID(),
                cmd.sagaInstanceId(),
                cmd.orderId(),
                reservationId,
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                event.eventId(),
                "StockReservation",
                cmd.sagaInstanceId(),
                "StockReserved",
                TOPIC_INVENTORY_EVENTS,
                event,
                cmd.sagaInstanceId()
        );
        outboxRepo.save(outbox);

        return Result.RESERVED;
    }

    /**
     * Reserve rollback'inden sonra StockReservationFailed event'i ayrı tx'te emit eder.
     * processed_events insert'i de burada — reserve tx rollback olduğu için orada yazılan
     * K1 satırı geri alındı; aynı commandId yeniden gelirse bu sefer Failed yolu hit eder
     * ve idempotent kalır (ON CONFLICT yutar).
     */
    @Transactional
    public void emitFailed(InventoryEvents.ReserveStock cmd, String reason) {
        OffsetDateTime now = OffsetDateTime.now();

        processedRepo.insertIfAbsent(cmd.commandId(), CONSUMER_NAME, TOPIC_INVENTORY_COMMANDS, now);

        InventoryEvents.StockReservationFailed event = new InventoryEvents.StockReservationFailed(
                UUID.randomUUID(),
                cmd.sagaInstanceId(),
                cmd.orderId(),
                reason,
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                event.eventId(),
                "StockReservation",
                cmd.sagaInstanceId(),
                "StockReservationFailed",
                TOPIC_INVENTORY_EVENTS,
                event,
                cmd.sagaInstanceId()
        );
        outboxRepo.save(outbox);
    }

    /**
     * ReleaseStock compensation — rezerve edilen stoğu geri ver (reserved→available).
     * Saga'nın tüm reservation satırlarını sagaInstanceId üzerinden tarar.
     * NOT: Mevcut 2-adımlı zincirde tetiklenmiyor (stoktan sonra adım yok); simetri
     * için ve ileride çok-adımlı saga'da kullanılmak üzere hazır.
     */
    @Transactional
    public ReleaseResult release(InventoryEvents.ReleaseStock cmd) {
        OffsetDateTime now = OffsetDateTime.now();

        // K1 — processed_events guard.
        int inserted = processedRepo.insertIfAbsent(
                cmd.commandId(), CONSUMER_NAME, TOPIC_INVENTORY_COMMANDS, now);
        if (inserted == 0) {
            log.info("K1 hit: release commandId={} already processed, skipping", cmd.commandId());
            return ReleaseResult.SKIPPED_DUPLICATE;
        }

        List<StockReservation> rows = reservationRepo.findBySagaInstanceId(cmd.sagaInstanceId());
        int released = 0;
        for (StockReservation row : rows) {
            if (!"RESERVED".equals(row.getStatus())) {
                continue;  // zaten RELEASED/CONFIRMED — idempotent skip
            }
            InventoryItem inv = invRepo.findByProductIdForUpdate(row.getProductId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Reservation references unknown product: " + row.getProductId()));
            inv.setAvailableQuantity(inv.getAvailableQuantity() + row.getQuantity());
            inv.setReservedQuantity(inv.getReservedQuantity() - row.getQuantity());
            inv.setUpdatedAt(now);

            row.setStatus("RELEASED");
            row.setReleasedAt(now);
            released++;
        }

        if (released == 0) {
            log.warn("Nothing to release for saga={}, no StockReleased emitted", cmd.sagaInstanceId());
            return ReleaseResult.SKIPPED_NOTHING_TO_RELEASE;
        }

        InventoryEvents.StockReleased event = new InventoryEvents.StockReleased(
                UUID.randomUUID(),
                cmd.sagaInstanceId(),
                cmd.reservationId(),
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                event.eventId(),
                "StockReservation",
                cmd.sagaInstanceId(),
                "StockReleased",
                TOPIC_INVENTORY_EVENTS,
                event,
                cmd.sagaInstanceId()
        );
        outboxRepo.save(outbox);

        log.info("Released {} reservation(s) for saga={}", released, cmd.sagaInstanceId());
        return ReleaseResult.RELEASED;
    }
}
