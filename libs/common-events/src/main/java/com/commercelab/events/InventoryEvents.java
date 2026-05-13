package com.commercelab.events;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Inventory domain command/event'leri.
 * Topic eşlemesi (ADR-001):
 *   - ReserveStock, ReleaseStock → inventory.commands (saga publish, inventory consume)
 *   - StockReserved, StockReservationFailed, StockReleased → inventory.events (inventory publish)
 */
public final class InventoryEvents {
    private InventoryEvents() {}

    public record ReserveStock(
        UUID commandId,
        UUID sagaInstanceId,
        UUID orderId,
        List<OrderItem> items,
        Instant requestedAt
    ) {}

    public record ReleaseStock(
        UUID commandId,
        UUID sagaInstanceId,
        UUID reservationId,
        Instant requestedAt
    ) {}

    public record StockReserved(
        UUID eventId,
        UUID sagaInstanceId,
        UUID orderId,
        UUID reservationId,
        Instant occurredAt
    ) {}

    public record StockReservationFailed(
        UUID eventId,
        UUID sagaInstanceId,
        UUID orderId,
        String reason,
        Instant occurredAt
    ) {}

    public record StockReleased(
        UUID eventId,
        UUID sagaInstanceId,
        UUID reservationId,
        Instant occurredAt
    ) {}
}
