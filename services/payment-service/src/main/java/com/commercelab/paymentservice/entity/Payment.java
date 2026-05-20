package com.commercelab.paymentservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Bir payment satırı = bir saga'nın ödeme kaydı.
 * saga_instance_id UNIQUE = domain-level idempotency (K2).
 * Gün 1 STUB: status=CAPTURED, stripePaymentIntentId=null. Gün 2 gerçek Stripe doldurur.
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "saga_instance_id", nullable = false)
    private UUID sagaInstanceId;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "stripe_payment_intent_id", length = 128)
    private String stripePaymentIntentId;

    @Column(name = "status", nullable = false, length = 32)
    private String status;  // AUTHORIZED, CAPTURED, FAILED, CANCELLED

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "captured_at")
    private OffsetDateTime capturedAt;
}
