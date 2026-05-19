package com.commercelab.inventoryservice.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Stok tablosu — bir ürün için available + reserved miktarları.
 * Pessimistic write lock üzerinden tek satır eş zamanlı yalnız bir transaction
 * tarafından güncellenebilir (oversell koruması).
 */
@Entity
@Table(name = "inventory_items")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryItem {

    @Id
    @Column(name = "product_id", nullable = false, updatable = false)
    private UUID productId;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private int reservedQuantity;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;
}
