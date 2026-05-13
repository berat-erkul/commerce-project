package com.commercelab.events;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Sipariş kalemi — order, saga ve inventory event/command'larında ortak kullanılır.
 * Inventory tarafı unitPrice'i kullanmaz ama paylaşım için tek schema tutuyoruz.
 */
public record OrderItem(
    UUID productId,
    int quantity,
    BigDecimal unitPrice
) {}
