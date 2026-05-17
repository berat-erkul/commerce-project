package com.commercelab.sagaservice.service;

import com.commercelab.events.PaymentEvents;
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

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * PaymentCompleted → RESERVE_STOCK forward advance.
 * Connection scope = bu metodun süresi. Pessimistic lock + state guard + 2-katman idempotency.
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaAdvancePersister {

    static final String CONSUMER_NAME = "saga-service-payment";
    static final String TOPIC_PAYMENT_EVENTS = "payment.events";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final SagaAdvanceFactory factory;

    enum Result { ADVANCED, SKIPPED_DUPLICATE_EVENT, SKIPPED_UNKNOWN_SAGA, SKIPPED_INVALID_STATE, SKIPPED_NO_ACTIVE_STEP }

    @Transactional
    Result persistAdvanceToReserveStock(UUID eventId, PaymentEvents.PaymentCompleted event) {
        // K1 — event-level idempotency
        if (processedRepo.findById(eventId).isPresent()) {
            return Result.SKIPPED_DUPLICATE_EVENT;
        }

        // Pessimistic lock — concurrent advance race koruması
        Optional<SagaInstance> opt = sagaRepo.findByIdForUpdate(event.sagaInstanceId());
        if (opt.isEmpty()) {
            log.warn("PaymentCompleted for unknown sagaInstanceId={}", event.sagaInstanceId());
            return Result.SKIPPED_UNKNOWN_SAGA;
        }
        SagaInstance saga = opt.get();

        // State guard — kolon otorite
        if (saga.getStatus() != SagaStatus.AWAITING_PAYMENT) {
            log.info("Saga {} in unexpected state {} for PaymentCompleted, skipping",
                    saga.getId(), saga.getStatus());
            recordProcessed(eventId);
            return Result.SKIPPED_INVALID_STATE;
        }

        // Aktif STARTED FORWARD PROCESS_PAYMENT step'i bul ve COMPLETED işaretle
        Optional<SagaStepHistory> stepOpt = stepRepo
                .findFirstBySagaInstanceIdAndStepNameAndStepTypeAndStatusOrderByStartedAtDesc(
                        saga.getId(), StepName.PROCESS_PAYMENT, StepType.FORWARD, StepStatus.STARTED);
        if (stepOpt.isEmpty()) {
            log.warn("No active PROCESS_PAYMENT step found for saga={}", saga.getId());
            return Result.SKIPPED_NO_ACTIVE_STEP;
        }
        OffsetDateTime now = OffsetDateTime.now();
        SagaStepHistory currentStep = stepOpt.get();
        currentStep.setStatus(StepStatus.COMPLETED);
        currentStep.setCompletedAt(now);

        // Factory CPU iş — yeni step + ReserveStock outbox
        SagaAdvanceFactory.AdvanceAggregate agg = factory.buildAdvanceToReserveStock(saga);

        // Saga state ilerlet
        saga.setStatus(SagaStatus.AWAITING_STOCK);
        saga.setCurrentStep(StepName.RESERVE_STOCK);
        saga.setUpdatedAt(now);

        stepRepo.save(agg.newStep());
        outboxRepo.save(agg.commandOutbox());
        recordProcessed(eventId);

        return Result.ADVANCED;
    }

    private void recordProcessed(UUID eventId) {
        processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_PAYMENT_EVENTS, OffsetDateTime.now());
    }
}
