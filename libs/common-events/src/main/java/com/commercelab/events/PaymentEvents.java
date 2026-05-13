package com.commercelab.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Payment domain command/event'leri.
 * Topic eşlemesi (ADR-001):
 *   - ProcessPayment, RefundPayment → payment.commands (saga publish, payment consume)
 *   - PaymentCompleted, PaymentFailed, PaymentRefunded → payment.events (payment publish)
 */
public final class PaymentEvents {
    private PaymentEvents() {}

    /**
     * Saga → payment: "şu siparişin ödemesini yap". sagaInstanceId domain-level
     * idempotency key (payments.saga_instance_id UNIQUE).
     */
    public record ProcessPayment(
        UUID commandId,
        UUID sagaInstanceId,
        UUID orderId,
        BigDecimal amount,
        String currency,
        Instant requestedAt
    ) {}

    /**
     * Saga → payment: "şu ödemeyi geri al". refundId saga tarafında üretilir
     * (compensation idempotency key — refunds.payment_id UNIQUE constraint'iyle
     * defense in depth).
     */
    public record RefundPayment(
        UUID commandId,
        UUID sagaInstanceId,
        UUID paymentId,
        UUID refundId,
        BigDecimal amount,
        Instant requestedAt
    ) {}

    public record PaymentCompleted(
        UUID eventId,
        UUID sagaInstanceId,
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        String stripePaymentIntentId,
        Instant occurredAt
    ) {}

    public record PaymentFailed(
        UUID eventId,
        UUID sagaInstanceId,
        UUID orderId,
        String reason,
        Instant occurredAt
    ) {}

    public record PaymentRefunded(
        UUID eventId,
        UUID sagaInstanceId,
        UUID paymentId,
        UUID refundId,
        BigDecimal amount,
        Instant occurredAt
    ) {}
}
