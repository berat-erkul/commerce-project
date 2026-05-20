package com.commercelab.paymentservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Bir payment'ın iadesi (compensation hattı). id = saga'nın ürettiği refundId
 * (idempotency anahtarı). payment_id UNIQUE = bir payment'a en fazla 1 refund.
 */
@Entity
@Table(name = "refunds")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Refund {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "payment_id", nullable = false)
    private UUID paymentId;

    @Column(name = "refund_id_external", length = 128)
    private String refundIdExternal;  // re_xxx (Stripe) — STUB'da null

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "status", nullable = false, length = 32)
    private String status;  // PENDING, COMPLETED, FAILED

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;
}
