package com.commercelab.inventoryservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Bir saga'nın bir ürün için yaptığı rezervasyon.
 * V2 sonrası UNIQUE(saga_instance_id, product_id) — saga başına çok kalem desteklenir,
 * aynı (saga, product) çifti tekildir (K2 idempotency).
 */
@Entity
@Table(name = "stock_reservations")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class StockReservation {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "saga_instance_id", nullable = false)
    private UUID sagaInstanceId;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    @Column(name = "status", nullable = false, length = 32)
    private String status;  // RESERVED, RELEASED, CONFIRMED

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "released_at")
    private OffsetDateTime releasedAt;
}
