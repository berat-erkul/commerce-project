package com.commercelab.sagaservice.service;

import com.commercelab.events.OrderEvents;
import com.commercelab.events.PaymentEvents;
import com.commercelab.sagaservice.entity.OutboxEvent;
import com.commercelab.sagaservice.entity.SagaInstance;
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
 * PaymentRefunded → terminal CANCELLED. Compensation döngüsünün KAPANIŞI.
 * <p>
 * COMPENSATING state'inde RefundPayment'in karşılığı PaymentRefunded geldi:
 * para iade edildi, compensation tamam. Saga CANCELLED'a düşer + OrderCancelled
 * basılır (order-service order.status=CANCELLED yapsın).
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaCancelPersister {

    static final String CONSUMER_NAME = "saga-service-payment";
    static final String TOPIC_PAYMENT_EVENTS = "payment.events";
    static final String TOPIC_ORDER_EVENTS = "order.events";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    enum Result { CANCELLED, SKIPPED_DUPLICATE_EVENT, SKIPPED_UNKNOWN_SAGA, SKIPPED_INVALID_STATE }

    @Transactional
    Result cancelOnPaymentRefunded(UUID eventId, PaymentEvents.PaymentRefunded event) {
        // K1
        if (processedRepo.findById(eventId).isPresent()) {
            return Result.SKIPPED_DUPLICATE_EVENT;
        }

        Optional<SagaInstance> opt = sagaRepo.findByIdForUpdate(event.sagaInstanceId());
        if (opt.isEmpty()) {
            log.warn("PaymentRefunded for unknown sagaInstanceId={}", event.sagaInstanceId());
            return Result.SKIPPED_UNKNOWN_SAGA;
        }
        SagaInstance saga = opt.get();

        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            log.info("Saga {} in unexpected state {} for PaymentRefunded, skipping",
                    saga.getId(), saga.getStatus());
            recordProcessed(eventId);
            return Result.SKIPPED_INVALID_STATE;
        }

        OffsetDateTime now = OffsetDateTime.now();

        // PROCESS_PAYMENT / COMPENSATION step'i COMPLETED işaretle (refund tamamlandı)
        stepRepo.findFirstBySagaInstanceIdAndStepNameAndStepTypeAndStatusOrderByStartedAtDesc(
                        saga.getId(), StepName.PROCESS_PAYMENT, StepType.COMPENSATION, StepStatus.STARTED)
                .ifPresent(step -> {
                    step.setStatus(StepStatus.COMPLETED);
                    step.setCompletedAt(now);
                });

        // Saga terminal CANCELLED
        saga.setStatus(SagaStatus.CANCELLED);
        saga.setCompletedAt(now);
        saga.setUpdatedAt(now);

        // Terminal notification → order-service
        UUID notificationId = UUID.randomUUID();
        OrderEvents.OrderCancelled notification = new OrderEvents.OrderCancelled(
                notificationId,
                saga.getOrderId(),
                "Compensated: stock failed, payment refunded",
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                notificationId,
                "Order",
                saga.getOrderId(),
                "OrderCancelled",
                TOPIC_ORDER_EVENTS,
                notification,
                saga.getId()
        );
        outboxRepo.save(outbox);

        recordProcessed(eventId);
        return Result.CANCELLED;
    }

    private void recordProcessed(UUID eventId) {
        processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_PAYMENT_EVENTS, OffsetDateTime.now());
    }
}
