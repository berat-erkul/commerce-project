package com.commercelab.sagaservice.service;

import com.commercelab.events.InventoryEvents;
import com.commercelab.events.PaymentEvents;
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
 * StockReservationFailed → COMPENSATING + RefundPayment.
 * <p>
 * Compensation BAŞLANGICI (terminal değil). PROCESS_PAYMENT forward step'i
 * COMPLETED idi → para çekildi → geri ödenmesi şart. RefundCompleted geldiğinde
 * Gün 2'de yazılacak listener saga'yı CANCELLED'a düşürecek + OrderCancelled basacak.
 * <p>
 * paymentId saga.payload'dan okunur (AdvancePersister PaymentCompleted'da yazmıştı).
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaCompensatePersister {

    static final String CONSUMER_NAME = "saga-service-inventory";
    static final String TOPIC_INVENTORY_EVENTS = "inventory.events";
    static final String TOPIC_PAYMENT_COMMANDS = "payment.commands";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    enum Result { COMPENSATION_STARTED, SKIPPED_DUPLICATE_EVENT, SKIPPED_UNKNOWN_SAGA, SKIPPED_INVALID_STATE, SKIPPED_MISSING_PAYMENT_ID }

    @Transactional
    Result compensateOnStockReservationFailed(UUID eventId, InventoryEvents.StockReservationFailed event) {
        // K1
        if (processedRepo.findById(eventId).isPresent()) {
            return Result.SKIPPED_DUPLICATE_EVENT;
        }

        Optional<SagaInstance> opt = sagaRepo.findByIdForUpdate(event.sagaInstanceId());
        if (opt.isEmpty()) {
            log.warn("StockReservationFailed for unknown sagaInstanceId={}", event.sagaInstanceId());
            return Result.SKIPPED_UNKNOWN_SAGA;
        }
        SagaInstance saga = opt.get();

        if (saga.getStatus() != SagaStatus.AWAITING_STOCK) {
            log.info("Saga {} in unexpected state {} for StockReservationFailed, skipping",
                    saga.getId(), saga.getStatus());
            recordProcessed(eventId);
            return Result.SKIPPED_INVALID_STATE;
        }

        // paymentId zorunlu — yoksa MANUAL_INTERVENTION (alarm). Saga AdvancePersister'dan geçtiyse var olmalı.
        UUID paymentId = saga.getPayload().getPaymentId();
        if (paymentId == null) {
            log.error("Compensation impossible: paymentId missing for saga={}. Marking MANUAL_INTERVENTION.",
                    saga.getId());
            saga.setStatus(SagaStatus.MANUAL_INTERVENTION);
            saga.setLastError("Stock failed but paymentId missing — manual refund required");
            saga.setUpdatedAt(OffsetDateTime.now());
            recordProcessed(eventId);
            return Result.SKIPPED_MISSING_PAYMENT_ID;
        }

        OffsetDateTime now = OffsetDateTime.now();

        // RESERVE_STOCK forward step'i FAILED işaretle
        stepRepo.findFirstBySagaInstanceIdAndStepNameAndStepTypeAndStatusOrderByStartedAtDesc(
                        saga.getId(), StepName.RESERVE_STOCK, StepType.FORWARD, StepStatus.STARTED)
                .ifPresent(step -> {
                    step.setStatus(StepStatus.FAILED);
                    step.setCompletedAt(now);
                });

        // Compensation step ekle: PROCESS_PAYMENT / COMPENSATION / STARTED
        SagaStepHistory compensationStep = SagaStepHistory.builder()
                .id(UUID.randomUUID())
                .sagaInstanceId(saga.getId())
                .stepName(StepName.PROCESS_PAYMENT)
                .stepType(StepType.COMPENSATION)
                .status(StepStatus.STARTED)
                .startedAt(now)
                .build();

        // RefundPayment command outbox satırı
        UUID commandId = UUID.randomUUID();
        UUID refundId = UUID.randomUUID(); // saga-side idempotency key (payment-service: refunds.payment_id UNIQUE)
        PaymentEvents.RefundPayment cmd = new PaymentEvents.RefundPayment(
                commandId,
                saga.getId(),
                paymentId,
                refundId,
                saga.getPayload().getAmount(),
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                commandId,
                "Saga",
                saga.getId(),
                "RefundPayment",
                TOPIC_PAYMENT_COMMANDS,
                cmd,
                saga.getId()
        );

        // Saga state → COMPENSATING
        saga.setStatus(SagaStatus.COMPENSATING);
        saga.setCurrentStep(StepName.PROCESS_PAYMENT); // currently compensating PROCESS_PAYMENT
        saga.setLastError("Stock reservation failed: " + event.reason());
        saga.setUpdatedAt(now);

        stepRepo.save(compensationStep);
        outboxRepo.save(outbox);
        recordProcessed(eventId);

        return Result.COMPENSATION_STARTED;
    }

    private void recordProcessed(UUID eventId) {
        processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_INVENTORY_EVENTS, OffsetDateTime.now());
    }
}
