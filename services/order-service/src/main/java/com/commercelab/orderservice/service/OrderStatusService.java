package com.commercelab.orderservice.service;

import com.commercelab.orderservice.entity.Order;
import com.commercelab.orderservice.repo.IOrderRepository;
import com.commercelab.orderservice.repo.IProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * Saga'nın terminal event'lerine göre sipariş durumunu günceller.
 * K1 idempotency (processed_events ON CONFLICT). order-service'in ilk consumer rolü:
 * cross-service state değişikliği saga'dan event ile gelir, polling YOK (L4 üçüncü altın kural).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderStatusService {

    static final String CONSUMER_NAME = "order-service-events";
    static final String TOPIC_ORDER_EVENTS = "order.events";

    private final IOrderRepository orderRepo;
    private final IProcessedEventRepository processedRepo;

    public enum Result { UPDATED, SKIPPED_DUPLICATE, SKIPPED_UNKNOWN_ORDER }

    @Transactional
    public Result markCompleted(UUID eventId, UUID orderId) {
        return applyStatus(eventId, orderId, "COMPLETED");
    }

    @Transactional
    public Result markCancelled(UUID eventId, UUID orderId) {
        return applyStatus(eventId, orderId, "CANCELLED");
    }

    private Result applyStatus(UUID eventId, UUID orderId, String status) {
        OffsetDateTime now = OffsetDateTime.now();

        // K1 — event-level idempotency.
        int inserted = processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_ORDER_EVENTS, now);
        if (inserted == 0) {
            log.info("K1 hit: eventId={} already processed, skipping", eventId);
            return Result.SKIPPED_DUPLICATE;
        }

        Optional<Order> opt = orderRepo.findById(orderId);
        if (opt.isEmpty()) {
            log.warn("Terminal event for unknown orderId={}, status={}", orderId, status);
            return Result.SKIPPED_UNKNOWN_ORDER;
        }

        Order order = opt.get();
        order.setStatus(status);
        order.setUpdatedAt(now);
        log.info("Order {} -> {}", orderId, status);
        return Result.UPDATED;
    }
}
