package com.commercelab.sagaservice.service;

import com.commercelab.events.InventoryEvents;
import com.commercelab.events.OrderEvents;
import com.commercelab.sagaservice.entity.OutboxEvent;
import com.commercelab.sagaservice.entity.SagaInstance;
import com.commercelab.sagaservice.entity.SagaStepHistory;
import com.commercelab.sagaservice.entity.enums.SagaStatus;
import com.commercelab.sagaservice.entity.enums.StepName;
import com.commercelab.sagaservice.entity.enums.StepStatus;
import com.commercelab.sagaservice.entity.enums.StepType;
import com.commercelab.sagaservice.repo.IOutboxEventRepository;
import com.commercelab.sagaservice.repo.IProcessedEventRepository;
import com.commercelab.sagaservice.repo.ISagaInstanceRepository;
import com.commercelab.sagaservice.repo.ISagaStepHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * StockReserved → terminal COMPLETED + OrderCompleted notification.
 * Saga'nın happy-path sonu. OrderCancelled gibi, OrderCompleted da choreography
 * fan-out için order.events'e basılır (order-service order.status=COMPLETED yapsın).
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaCompletePersister {

    static final String CONSUMER_NAME = "saga-service-inventory";
    static final String TOPIC_INVENTORY_EVENTS = "inventory.events";
    static final String TOPIC_ORDER_EVENTS = "order.events";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    enum Result { COMPLETED, SKIPPED_DUPLICATE_EVENT, SKIPPED_UNKNOWN_SAGA, SKIPPED_INVALID_STATE, SKIPPED_NO_ACTIVE_STEP }

    @Transactional
    Result completeOnStockReserved(UUID eventId, InventoryEvents.StockReserved event) {
        // K1
        if (processedRepo.findById(eventId).isPresent()) {
            return Result.SKIPPED_DUPLICATE_EVENT;
        }

        Optional<SagaInstance> opt = sagaRepo.findByIdForUpdate(event.sagaInstanceId());
        if (opt.isEmpty()) {
            log.warn("StockReserved for unknown sagaInstanceId={}", event.sagaInstanceId());
            return Result.SKIPPED_UNKNOWN_SAGA;
        }
        SagaInstance saga = opt.get();

        if (saga.getStatus() != SagaStatus.AWAITING_STOCK) {
            log.info("Saga {} in unexpected state {} for StockReserved, skipping",
                    saga.getId(), saga.getStatus());
            recordProcessed(eventId);
            return Result.SKIPPED_INVALID_STATE;
        }

        // Aktif STARTED FORWARD RESERVE_STOCK step'i COMPLETED işaretle
        Optional<SagaStepHistory> stepOpt = stepRepo
                .findFirstBySagaInstanceIdAndStepNameAndStepTypeAndStatusOrderByStartedAtDesc(
                        saga.getId(), StepName.RESERVE_STOCK, StepType.FORWARD, StepStatus.STARTED);
        if (stepOpt.isEmpty()) {
            log.warn("No active RESERVE_STOCK step found for saga={}", saga.getId());
            return Result.SKIPPED_NO_ACTIVE_STEP;
        }
        OffsetDateTime now = OffsetDateTime.now();
        SagaStepHistory currentStep = stepOpt.get();
        currentStep.setStatus(StepStatus.COMPLETED);
        currentStep.setCompletedAt(now);

        // reservationId memory'e yaz (compensation gelmeyecek ama audit için faydalı)
        saga.getPayload().setReservationId(event.reservationId());
        saga.setPayload(saga.getPayload());

        // Saga terminal COMPLETED
        saga.setStatus(SagaStatus.COMPLETED);
        saga.setCompletedAt(now);
        saga.setUpdatedAt(now);

        // Terminal notification — order-service için OrderCompleted.
        // aggregateId = orderId (OrderCreated ile aynı partition, ordering korunur).
        UUID notificationId = UUID.randomUUID();
        OrderEvents.OrderCompleted notification = new OrderEvents.OrderCompleted(
                notificationId,
                saga.getOrderId(),
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                notificationId,
                "Order",
                saga.getOrderId(),
                "OrderCompleted",
                TOPIC_ORDER_EVENTS,
                notification,
                saga.getId()
        );
        outboxRepo.save(outbox);

        recordProcessed(eventId);
        return Result.COMPLETED;
    }

    private void recordProcessed(UUID eventId) {
        processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_INVENTORY_EVENTS, OffsetDateTime.now());
    }
}
