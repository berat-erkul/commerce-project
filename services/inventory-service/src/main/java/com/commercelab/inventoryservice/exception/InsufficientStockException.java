package com.commercelab.inventoryservice.exception;

/**
 * Reserve sırasında bir item için stok yetmediğinde fırlatılır.
 * @Transactional sınır dışına propagate olur → Spring rollback → tüm reservation iptal.
 * Listener yakalar ve ayrı tx'te StockReservationFailed event'i emit eder.
 */
public class InsufficientStockException extends RuntimeException {
    public InsufficientStockException(String message) {
        super(message);
    }
}
