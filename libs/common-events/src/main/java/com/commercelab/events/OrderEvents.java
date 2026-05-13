package com.commercelab.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * order-service tarafından yayınlanan event'ler ve order tarafına gelen saga command'ları.
 * Topic eşlemesi (ADR-001):
 *   - OrderCreated, OrderCompleted, OrderCancelled → order.events
 *   - StartOrderSaga → saga.commands
 */
public final class OrderEvents {
    private OrderEvents() {}

    public record OrderCreated(
        UUID eventId,
        UUID orderId,
        UUID customerId,
        BigDecimal totalAmount,
        String currency,
        List<OrderItem> items,
        Instant occurredAt
    ) {}

    public record OrderCompleted(
        UUID eventId,
        UUID orderId,
        Instant occurredAt
    ) {}

    public record OrderCancelled(
        UUID eventId,
        UUID orderId,
        String reason,
        Instant occurredAt
    ) {}

    /**
     * Saga başlatma command'ı — order-service publish eder, saga-service consume eder.
     * (Şimdilik OrderCreated event'i de saga'yı tetikleyebiliyor; bu command alternatif
     *  giriş yolu olarak rezerve — örn. admin manuel retry, return saga'sı, vb.)
     */
    public record StartOrderSaga(
        UUID commandId,
        UUID orderId,
        BigDecimal totalAmount,
        String currency,
        List<OrderItem> items,
        Instant requestedAt
    ) {}
}
